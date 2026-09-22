//! 基于 DashMap 的内存状态机 (In-Memory State Store)
//!
//! # 并发设计
//! [`dashmap::DashMap`] 将哈希表切分为多个独立加锁的分片（shard），
//! 对不同 key 的并发读写落在不同分片上即互不阻塞，相较单把 `Mutex<HashMap>` 显著降低锁竞争。
//! 因此本存储可在“上千机器人高频心跳上报 + 调度器并发读写”下保持低延迟。
//!
//! # 快照语义
//! 读接口返回 [`RobotState`] 的克隆快照，避免向调用方泄漏 `DashMap` 的 `Ref` 守卫，
//! 防止跨分片死锁并简化上层使用心智。

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use thiserror::Error;

use crate::domain::{
    Heartbeat, Location, NodeId, RobotId, RobotState, RobotStatus, Task, TaskId, TaskStatus,
    TimestampMs,
};

/// 单个轨迹采样点：某时刻机器人所在位置。
pub type TrailPoint = (TimestampMs, Location);

/// 每辆机器人保留的最大轨迹点数（防止无限增长，环形缓冲）。
const TRAIL_CAPACITY: usize = 64;

/// 存储层错误。
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("机器人不存在: {0}")]
    RobotNotFound(RobotId),
    #[error("任务不存在: {0}")]
    TaskNotFound(TaskId),
    #[error("机器人 {0} 当前不可分配（非空闲）")]
    RobotNotAvailable(RobotId),
}

/// 一次超时/离线回收的结果：被重新入队（回 Pending）与被判定失败（转 Failed）的任务。
#[derive(Debug, Clone, Default)]
pub struct ReclaimOutcome {
    /// 已退回 Pending、需重新入优先队列的任务
    pub requeued: Vec<TaskId>,
    /// 因达到重试上限而转 Failed 的任务
    pub failed: Vec<TaskId>,
}

/// 高并发内存状态存储。
///
/// # 共享语义
/// 内部以 `Arc<StoreInner>` 承载真实的 `DashMap` 与原子计数器，因此 `MemoryStore: Clone`
/// 是**廉价且共享**的：所有克隆句柄指向同一份状态。这正是本系统的用法——
/// 调度器、服务端、main 各持一个 `clone`，读写的都是同一份“事实来源”。
#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<StoreInner>,
}

/// 真正的存储实体：分片哈希表 + 免锁原子计数。
pub struct StoreInner {
    /// 机器人实时状态表
    robots: DashMap<RobotId, RobotState>,
    /// 任务状态表
    tasks: DashMap<TaskId, Task>,
    /// 每辆机器人的近期轨迹（位置时序，供轨迹可视化）
    trails: DashMap<RobotId, VecDeque<TrailPoint>>,
    /// 统计计数（原子，免锁）
    heartbeat_count: AtomicU64,
    task_submit_count: AtomicU64,
    assigned_count: AtomicU64,
    completed_count: AtomicU64,
    /// 累计自动解除的死锁环数（牺牲者假释次数）
    deadlock_resolved: AtomicU64,
    /// 累计因超时/离线被回收重试的任务数
    task_retried: AtomicU64,
    /// 累计因超过重试上限而判定失败的任务数
    task_failed: AtomicU64,
}

impl Default for StoreInner {
    fn default() -> Self {
        Self {
            robots: DashMap::new(),
            tasks: DashMap::new(),
            trails: DashMap::new(),
            heartbeat_count: AtomicU64::new(0),
            task_submit_count: AtomicU64::new(0),
            assigned_count: AtomicU64::new(0),
            completed_count: AtomicU64::new(0),
            deadlock_resolved: AtomicU64::new(0),
            task_retried: AtomicU64::new(0),
            task_failed: AtomicU64::new(0),
        }
    }
}

/// 通过 `Deref` 把 `MemoryStore` 透明指向 `StoreInner`，
/// 使 `self.robots` / `self.tasks` 等字段访问无需层层解引用。
impl std::ops::Deref for MemoryStore {
    type Target = StoreInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl MemoryStore {
    /// 创建空存储。
    pub fn new() -> Self {
        Self::default()
    }

    // ------------------------------------------------------------------ 机器人
    /// 注册（或幂等覆盖注册）一台机器人。
    pub fn register_robot(&self, state: RobotState) {
        self.push_trail(state.id, state.last_heartbeat, state.location);
        self.robots.insert(state.id, state);
    }

    /// 追加一个轨迹采样点：仅当与上一个点位置不同才记录（去噪声），并维持环形上限。
    fn push_trail(&self, id: RobotId, t: TimestampMs, loc: Location) {
        let mut entry = self.trails.entry(id).or_default();
        let changed = match entry.back() {
            Some((_, last)) => {
                last.node != loc.node
                    || (last.x - loc.x).abs() > 0.01
                    || (last.y - loc.y).abs() > 0.01
            }
            None => true,
        };
        if changed {
            entry.push_back((t, loc));
            while entry.len() > TRAIL_CAPACITY {
                entry.pop_front();
            }
        }
    }

    /// 处理一次心跳上报：定位到对应分片、就地更新状态字段。
    ///
    /// 使用 `get_mut` 只在命中的单个分片上加锁，写完后立即释放，锁粒度极小。
    pub fn apply_heartbeat(&self, hb: Heartbeat) -> Result<(), StoreError> {
        match self.robots.get_mut(&hb.id) {
            Some(mut robot) => {
                // 节点发生变化时刷新“最近变更时刻”，供 GUI 逐段插值推算行驶速度。
                if robot.location.node != hb.location.node {
                    robot.last_change_ms = hb.timestamp;
                }
                robot.location = hb.location;
                robot.battery = hb.battery;
                robot.last_heartbeat = hb.timestamp;
                // 载货态直接信任上报（取货/送达由机器人自行切换）。
                robot.carrying = hb.carrying;
                // 仅在非执行态时信任上报状态，避免覆盖调度器刚写入的 Busy 状态
                if !matches!(robot.status, RobotStatus::Busy) {
                    robot.status = hb.status;
                }
                self.heartbeat_count.fetch_add(1, Ordering::Relaxed);
                self.push_trail(hb.id, hb.timestamp, hb.location);
                Ok(())
            }
            None => Err(StoreError::RobotNotFound(hb.id)),
        }
    }

    /// 获取机器人状态快照。
    pub fn get_robot(&self, id: RobotId) -> Option<RobotState> {
        self.robots.get(&id).map(|r| r.clone())
    }

    /// 收集所有处于可分配状态（Idle 且心跳未超时）的机器人快照。
    pub fn available_robots(&self, now: u64, timeout_ms: u64) -> Vec<RobotState> {
        let mut out = Vec::new();
        for entry in self.robots.iter() {
            let s = entry.value();
            if s.status.is_available() && !s.is_stale(now, timeout_ms) {
                out.push(s.clone());
            }
        }
        out
    }

    // ------------------------------------------------------------------ 任务
    /// 提交（写入）一个任务。
    pub fn submit_task(&self, task: Task) {
        self.task_submit_count.fetch_add(1, Ordering::Relaxed);
        self.tasks.insert(task.id, task);
    }

    /// 获取任务快照。
    pub fn get_task(&self, id: TaskId) -> Option<Task> {
        self.tasks.get(&id).map(|t| t.clone())
    }

    /// 收集所有待处理任务快照（用于调度器重建堆或诊断）。
    pub fn pending_tasks(&self) -> Vec<Task> {
        self.tasks
            .iter()
            .filter(|e| e.value().is_pending())
            .map(|e| e.value().clone())
            .collect()
    }

    // ------------------------------------------------------------------ 组合原子操作
    /// 将某任务分配给某机器人：一次操作内同时更新两张表的状态。
    ///
    /// 由单线程的调度器串行调用以作为“决策唯一入口”，因此跨表不存在并发竞态；
    /// 表内写入仍依赖 DashMap 的分片锁保证与心跳上报的互斥可见性。
    pub fn assign(
        &self,
        robot_id: RobotId,
        task_id: TaskId,
        route_target: Location,
        route: Vec<NodeId>,
    ) -> Result<(RobotState, Task), StoreError> {
        // 1) 锁定机器人分片并做状态流转。
        //    注意：不在此处把 location 直接改写到目的地（会造成“瞬移”）；
        //    location 仍由机器人逐节点上报的心跳推进，仅登记目的地与计划路线。
        {
            let mut robot = self
                .robots
                .get_mut(&robot_id)
                .ok_or(StoreError::RobotNotFound(robot_id))?;
            if !robot.status.is_available() {
                return Err(StoreError::RobotNotAvailable(robot_id));
            }
            robot.status = RobotStatus::Busy;
            robot.current_task = Some(task_id);
            robot.target = route_target.node;
            robot.route = route;
        } // 机器人 Ref 在此释放，避免与任务分片锁交叉持有

        self.assigned_count.fetch_add(1, Ordering::Relaxed);

        // 2) 更新任务状态
        let task_snapshot = {
            let mut task = self
                .tasks
                .get_mut(&task_id)
                .ok_or(StoreError::TaskNotFound(task_id))?;
            task.status = TaskStatus::Assigned;
            task.assigned_robot = Some(robot_id);
            task.assigned_at = crate::config::monotonic_ms();
            task.clone()
        };

        let robot_snapshot = self
            .get_robot(robot_id)
            .ok_or(StoreError::RobotNotFound(robot_id))?;
        Ok((robot_snapshot, task_snapshot))
    }

    /// 标记任务完成，并将对应机器人释放回空闲态。
    pub fn complete_task(&self, robot_id: RobotId, task_id: TaskId) -> Result<(), StoreError> {
        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.status = TaskStatus::Completed;
        } else {
            return Err(StoreError::TaskNotFound(task_id));
        }
        if let Some(mut robot) = self.robots.get_mut(&robot_id) {
            robot.current_task = None;
            robot.target = None;
            robot.route.clear();
            robot.carrying = false;
            robot.status = RobotStatus::Idle;
        }
        self.completed_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 回滚一次分配：机器人释放回 `Idle`（保留当前位置，清除绑定任务与目的地），
    /// 任务退回 `Pending`（清除指派）。用于下发通道拥塞时的“不丢任务”回滚，下 tick 会重试。
    pub fn unassign(&self, robot_id: RobotId, task_id: TaskId) {
        if let Some(mut robot) = self.robots.get_mut(&robot_id) {
            if matches!(robot.status, RobotStatus::Busy) {
                robot.status = RobotStatus::Idle;
            }
            robot.current_task = None;
            robot.target = None;
            robot.route.clear();
            robot.carrying = false;
        }
        if let Some(mut task) = self.tasks.get_mut(&task_id) {
            task.status = TaskStatus::Pending;
            task.assigned_robot = None;
            task.assigned_at = 0;
        }
    }

    /// 死锁解除：假释（parole）环内一台牺牲者机器人——等价于 [`unassign`]，
    /// 使其不再持有目的地、退出等待-有向图，从而打断环；并累计解除计数。其任务将在后续 tick 重排。
    pub fn parole_robot(&self, robot_id: RobotId, task_id: TaskId) {
        self.unassign(robot_id, task_id);
        self.deadlock_resolved.fetch_add(1, Ordering::Relaxed);
    }

    /// 超时/离线回收：扫描处于 `Assigned` 的任务，若分配后超时无进展或其绑定机器人已离线/心跳超时，
    /// 则按重试次数决定回 `Pending`（累加重试）还是转 `Failed`（达上限）；同时释放对应机器人。
    /// 由调度器单线程周期性调用。返回被重新入队与被判失败的任务 id 集合。
    pub fn reclaim_stale_tasks(
        &self,
        now: TimestampMs,
        timeout_ms: u64,
        max_retries: u8,
    ) -> ReclaimOutcome {
        let mut outcome = ReclaimOutcome::default();
        // 先收集需处理的 (task_id, robot)，避免持锁跨表迭代。
        let stale: Vec<(TaskId, Option<RobotId>, bool)> = self
            .tasks
            .iter()
            .filter(|e| {
                let t = e.value();
                matches!(t.status, TaskStatus::Assigned | TaskStatus::Executing)
            })
            .filter_map(|e| {
                let t = e.value();
                let robot = t.assigned_robot;
                // 触发回收：分配超时，或绑定机器人已不存在/离线/心跳超时。
                let timed_out =
                    t.assigned_at != 0 && now.saturating_sub(t.assigned_at) > timeout_ms;
                let robot_gone = match robot {
                    Some(rid) => match self.robots.get(&rid) {
                        Some(r) => r.status == RobotStatus::Offline || r.is_stale(now, timeout_ms),
                        None => true,
                    },
                    None => true,
                };
                (timed_out || robot_gone).then_some((t.id, robot, t.retry_count >= max_retries))
            })
            .collect();

        for (task_id, robot, exhausted) in stale {
            // 释放绑定机器人（若仍指向该任务）。
            if let Some(rid) = robot
                && let Some(mut r) = self.robots.get_mut(&rid)
                && r.current_task == Some(task_id)
            {
                r.current_task = None;
                r.target = None;
                r.route.clear();
                r.carrying = false;
                if r.status != RobotStatus::Offline {
                    r.status = RobotStatus::Idle;
                }
            }
            let mut action = None;
            if let Some(mut t) = self.tasks.get_mut(&task_id) {
                if exhausted {
                    t.status = TaskStatus::Failed;
                    t.assigned_robot = None;
                    t.assigned_at = 0;
                    action = Some(false);
                } else {
                    t.status = TaskStatus::Pending;
                    t.assigned_robot = None;
                    t.assigned_at = 0;
                    t.retry_count = t.retry_count.saturating_add(1);
                    action = Some(true);
                }
            }
            match action {
                Some(true) => {
                    self.task_retried.fetch_add(1, Ordering::Relaxed);
                    outcome.requeued.push(task_id);
                }
                Some(false) => {
                    self.task_failed.fetch_add(1, Ordering::Relaxed);
                    outcome.failed.push(task_id);
                }
                None => {}
            }
        }
        outcome
    }

    /// 心跳巡检：把超时未上报的机器人标记为离线（供调度器周期性调用）。
    pub fn evict_stale_robots(&self, now: u64, timeout_ms: u64) -> Vec<RobotId> {
        let mut stale = Vec::new();
        for entry in self.robots.iter() {
            if entry.value().is_stale(now, timeout_ms)
                && entry.value().status != RobotStatus::Offline
            {
                stale.push(*entry.key());
            }
        }
        for id in &stale {
            if let Some(mut r) = self.robots.get_mut(id) {
                r.status = RobotStatus::Offline;
                r.current_task = None;
                r.target = None;
                r.carrying = false;
            }
        }
        stale
    }

    // ------------------------------------------------------------------ 统计
    /// 导出全部机器人状态快照（供 GUI 渲染，逐分片短锁克隆）。
    pub fn all_robots(&self) -> Vec<RobotState> {
        self.robots.iter().map(|e| e.value().clone()).collect()
    }

    /// 导出全部任务快照（供 GUI 渲染）。
    pub fn all_tasks(&self) -> Vec<Task> {
        self.tasks.iter().map(|e| e.value().clone()).collect()
    }

    /// 当前在线（非离线）机器人数量。
    pub fn online_robot_count(&self) -> usize {
        self.robots
            .iter()
            .filter(|e| e.value().status != RobotStatus::Offline)
            .count()
    }

    /// 机器人总数。
    pub fn robot_count(&self) -> usize {
        self.robots.len()
    }

    /// 任务总数。
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// 累计处理心跳数。
    pub fn heartbeat_count(&self) -> u64 {
        self.heartbeat_count.load(Ordering::Relaxed)
    }

    /// 累计提交任务数。
    pub fn task_submit_count(&self) -> u64 {
        self.task_submit_count.load(Ordering::Relaxed)
    }

    /// 累计分配次数。
    pub fn assigned_count(&self) -> u64 {
        self.assigned_count.load(Ordering::Relaxed)
    }

    /// 累计完成次数。
    pub fn completed_count(&self) -> u64 {
        self.completed_count.load(Ordering::Relaxed)
    }

    /// 累计自动解除的死锁环数。
    pub fn deadlock_resolved_count(&self) -> u64 {
        self.deadlock_resolved.load(Ordering::Relaxed)
    }

    /// 累计被回收重试的任务数。
    pub fn task_retried_count(&self) -> u64 {
        self.task_retried.load(Ordering::Relaxed)
    }

    /// 累计因超重试上限而失败的任务数。
    pub fn task_failed_count(&self) -> u64 {
        self.task_failed.load(Ordering::Relaxed)
    }

    /// 当前待处理（Pending）任务数。
    pub fn pending_count(&self) -> usize {
        self.tasks.iter().filter(|e| e.value().is_pending()).count()
    }

    /// 导出一辆机器人的近期轨迹（时序， oldest → newest）。供 GUI 绘制。
    pub fn robot_trail(&self, id: RobotId) -> Vec<TrailPoint> {
        self.trails
            .get(&id)
            .map(|e| e.clone().into_iter().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Battery, NodeId, TaskKind, TaskPriority};

    fn loc(n: u32) -> Location {
        Location::at_node(NodeId(n), n as f32, 0.0)
    }

    fn onboard(store: &MemoryStore, id: u64) {
        store.register_robot(RobotState::onboard(
            RobotId(id),
            loc(1),
            Battery::new(80),
            0,
        ));
    }

    fn transport(store: &MemoryStore, tid: u64) {
        store.submit_task(Task::new(
            TaskId(tid),
            TaskKind::Transport {
                pickup: loc(2),
                dropoff: loc(3),
            },
            TaskPriority::Normal,
            0,
        ));
    }

    #[test]
    fn unassign_rolls_back_both_tables() {
        let store = MemoryStore::new();
        onboard(&store, 1);
        transport(&store, 100);

        store
            .assign(RobotId(1), TaskId(100), loc(2), vec![NodeId(2)])
            .expect("assign 应成功");
        assert_eq!(
            store.get_robot(RobotId(1)).unwrap().status,
            RobotStatus::Busy
        );
        assert_eq!(
            store.get_task(TaskId(100)).unwrap().status,
            TaskStatus::Assigned
        );

        store.unassign(RobotId(1), TaskId(100));
        let r = store.get_robot(RobotId(1)).unwrap();
        assert_eq!(r.status, RobotStatus::Idle);
        assert_eq!(r.current_task, None);
        assert_eq!(r.target, None);
        let t = store.get_task(TaskId(100)).unwrap();
        assert_eq!(t.status, TaskStatus::Pending);
        assert_eq!(t.assigned_robot, None);
    }

    #[test]
    fn reclaim_requeues_until_limit_then_fails() {
        let store = MemoryStore::new();
        onboard(&store, 1);
        transport(&store, 100);
        store
            .assign(RobotId(1), TaskId(100), loc(2), vec![NodeId(2)])
            .unwrap();
        // assigned_at 已写为当前单调时间；用一个很早的 now + 小 timeout 不会触发，
        // 改用足够大的 now 使 timeout 命中。max_retries=2。
        let now = crate::config::monotonic_ms() + 10_000_000;

        // 第 1 次回收：retry 0 -> 1，回 Pending（requeued）。
        let out = store.reclaim_stale_tasks(now, 8_000, 2);
        assert_eq!(out.requeued, vec![TaskId(100)]);
        assert!(out.failed.is_empty());
        assert_eq!(store.get_task(TaskId(100)).unwrap().retry_count, 1);
        assert_eq!(store.task_retried_count(), 1);

        // 重新分配并再回收两次，第 3 次因 retry>=max 转 Failed。
        store
            .assign(RobotId(1), TaskId(100), loc(2), vec![NodeId(2)])
            .unwrap();
        let out = store.reclaim_stale_tasks(now + 1, 8_000, 2);
        assert_eq!(out.requeued, vec![TaskId(100)]); // retry 1 -> 2
        store
            .assign(RobotId(1), TaskId(100), loc(2), vec![NodeId(2)])
            .unwrap();
        let out = store.reclaim_stale_tasks(now + 2, 8_000, 2);
        assert_eq!(out.failed, vec![TaskId(100)]); // retry 2 >= max -> Failed
        assert_eq!(
            store.get_task(TaskId(100)).unwrap().status,
            TaskStatus::Failed
        );
        assert_eq!(store.task_failed_count(), 1);
    }

    #[test]
    fn parole_increments_deadlock_counter() {
        let store = MemoryStore::new();
        onboard(&store, 7);
        transport(&store, 500);
        store
            .assign(RobotId(7), TaskId(500), loc(2), vec![NodeId(2)])
            .unwrap();
        store.parole_robot(RobotId(7), TaskId(500));
        assert_eq!(
            store.get_robot(RobotId(7)).unwrap().status,
            RobotStatus::Idle
        );
        assert_eq!(
            store.get_task(TaskId(500)).unwrap().status,
            TaskStatus::Pending
        );
        assert_eq!(store.deadlock_resolved_count(), 1);
    }
}

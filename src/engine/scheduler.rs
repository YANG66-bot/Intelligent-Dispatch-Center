//! 任务调度器 (Scheduler)
//!
//! # 架构
//! 采用“单决策者 + 事件驱动”模型：外部（transport / 业务）通过 [`SchedulerHandle`]
//! 以 [`SchedulerEvent`] 通道投递事件，调度器在后台 `tokio` 任务中串行消费并做出分配决策。
//! 因为分配决策只有一个消费者线程，跨 `DashMap` 两表的“机器人↔任务”绑定天然无竞态，
//! 无需分布式锁或大临界区，写入路径保持极短。
//!
//! # 匹配策略
//! 每 tick：
//! 1. **超时回收**：回收分配后无进展/机器人已离线的任务，按重试次数回 Pending 或判失败；
//! 2. **老化匹配**：按“有效优先级”（含防饥饿老化提升）排序待分配任务，贪心选距离取货点最近
//!    且不在让路冷却中的空闲机器人，规划路径后落库并下发；下发拥塞则回滚留待下 tick；
//! 3. **死锁解除**：对本帧全量快照跑 [`conflict::analyze`]，对检出的每个死锁环假释一台
//!    牺牲者（有效优先级最低、平手取 id 最大者）令其让路，并将其置入冷却窗口。
//!
//! 该策略为可替换的参考实现——替换为成本矩阵 + 匈牙利算法或拍卖算法时，仅需改动 `run_matching`。

use std::collections::HashMap;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::config::{SystemConfig, millis, monotonic_ms};
use crate::domain::{
    Location, Map, NodeId, Path, RobotId, RobotState, Task, TaskId, TaskKey, TaskKind, TaskPriority,
};
use crate::engine::{Dispatch, Pathfinder, SchedulerEvent, conflict};
use crate::storage::MemoryStore;

/// 调度器本体。`P` 为可插拔的路径规划算法。
pub struct Scheduler<P: Pathfinder> {
    store: MemoryStore,
    map: Arc<Map>,
    pathfinder: P,
    events: flume::Receiver<SchedulerEvent>,
    dispatch: flume::Sender<Dispatch>,
    config: SystemConfig,
}

/// 调度器的对外句柄（可 `Clone`），供其它任务/线程投递事件。
#[derive(Clone)]
pub struct SchedulerHandle {
    events: flume::Sender<SchedulerEvent>,
}

impl SchedulerHandle {
    /// 提交一个新任务。
    pub fn submit_task(&self, task: Task) -> Result<(), flume::SendError<SchedulerEvent>> {
        self.events.send(SchedulerEvent::TaskSubmitted(task))
    }

    /// 上报某机器人完成了某任务。
    pub fn report_complete(
        &self,
        robot: RobotId,
        task: TaskId,
    ) -> Result<(), flume::SendError<SchedulerEvent>> {
        self.events
            .send(SchedulerEvent::TaskCompleted { robot, task })
    }

    /// 触发优雅停机。
    pub fn shutdown(&self) {
        let _ = self.events.send(SchedulerEvent::Shutdown);
    }
}

impl<P: Pathfinder + 'static> Scheduler<P> {
    /// 构建调度器。
    ///
    /// 返回三元组：
    /// - [`Scheduler`]：需在后台 `tokio::spawn` 中运行 `run()`；
    /// - [`SchedulerHandle`]：对外事件投递句柄（可 `Clone`，传给 transport）；
    /// - `Receiver<Dispatch>`：调度结果的下游派发接收端（交给 transport 的“派发泵”）。
    pub fn new(
        store: MemoryStore,
        map: Arc<Map>,
        pathfinder: P,
        config: SystemConfig,
        capacity: usize,
    ) -> (Self, SchedulerHandle, flume::Receiver<Dispatch>) {
        // 事件通道：外部 -> 调度器；下发通道：调度器 -> transport 派发泵。均为有界，提供背压。
        let (ev_tx, ev_rx) = flume::bounded::<SchedulerEvent>(capacity);
        let (dp_tx, dp_rx) = flume::bounded::<Dispatch>(capacity);
        let scheduler = Self {
            store,
            map,
            pathfinder,
            events: ev_rx,
            dispatch: dp_tx,
            config,
        };
        let handle = SchedulerHandle { events: ev_tx };
        (scheduler, handle, dp_rx)
    }

    /// 后台主循环。应通过 `tokio::spawn(scheduler.run())` 启动。
    pub async fn run(self) {
        let Self {
            store,
            map,
            pathfinder,
            events,
            dispatch,
            config,
        } = self;

        // 待分配任务：以 id -> task 的表维护；每 tick 按有效优先级现场排序（便于老化改键）。
        let mut pending: HashMap<TaskId, Task> = HashMap::new();
        // 让路冷却表：`机器人 -> 冷却截止时刻`，窗口内不参与分配，避免刚假释又重排回同一死锁环。
        let mut cooldown: HashMap<RobotId, u64> = HashMap::new();

        let mut ticker = tokio::time::interval(millis(config.scheduler_tick_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        info!("调度器启动，tick={}ms", config.scheduler_tick_ms);

        loop {
            tokio::select! {
                // 事件驱动：处理流入事件
                ev = events.recv_async() => {
                    match ev {
                        Ok(SchedulerEvent::TaskSubmitted(task)) => {
                            // 先落入“事实来源”存储，assign/complete 才能跨表定位到任务
                            store.submit_task(task.clone());
                            pending.insert(task.id, task);
                        }
                        Ok(SchedulerEvent::TaskCompleted { robot, task }) => {
                            if let Err(e) = store.complete_task(robot, task) {
                                warn!("完成任务失败: {e}");
                            }
                        }
                        Ok(SchedulerEvent::Shutdown) | Err(_) => {
                            info!("调度器收到停机信号，退出主循环");
                            break;
                        }
                    }
                }
                // 定时驱动：回收 -> 匹配 -> 死锁解除
                _ = ticker.tick() => {
                    let now = monotonic_ms();

                    // 过期冷却惰性清除，让牺牲者机器人重新可分配。
                    cooldown.retain(|_, until| now < *until);

                    // 1) 超时/离线回收：被重新入队的任务回填 pending，供本 tick 重排。
                    let outcome =
                        store.reclaim_stale_tasks(now, config.task_timeout_ms, config.max_task_retries);
                    for tid in outcome.requeued {
                        if let Some(t) = store.get_task(tid) {
                            pending.insert(tid, t);
                        }
                    }
                    if !outcome.failed.is_empty() {
                        warn!("回收判定失败 {} 个任务（超重试上限）", outcome.failed.len());
                    }

                    // 2) 老化匹配分配。
                    run_matching(
                        &store,
                        &map,
                        &pathfinder,
                        &dispatch,
                        &config,
                        &mut pending,
                        &cooldown,
                        now,
                    );

                    // 3) 死锁自动解除（可配置关闭，则退化为仅可视化）。
                    if config.enable_deadlock_resolution {
                        resolve_deadlocks(&store, &config, &mut cooldown, now);
                    }
                }
            }
        }
    }
}

/// 取出任务的目标节点（机器人需要前往的第一个节点）。
#[inline]
fn task_target(task: &Task) -> Option<NodeId> {
    task.goal_node()
}

/// 单轮匹配：按有效优先级（含老化）排序，尽可能把任务分配给最近的可用空闲机器人。
#[allow(clippy::too_many_arguments)]
fn run_matching<P: Pathfinder + ?Sized>(
    store: &MemoryStore,
    map: &Map,
    pathfinder: &P,
    dispatch: &flume::Sender<Dispatch>,
    config: &SystemConfig,
    pending: &mut HashMap<TaskId, Task>,
    cooldown: &HashMap<RobotId, u64>,
    now: u64,
) {
    // 1) 剔除心跳超时的机器人
    let evicted = store.evict_stale_robots(now, config.heartbeat_timeout_ms);
    if !evicted.is_empty() {
        debug!("心跳巡检下线 {} 台机器人", evicted.len());
    }

    if pending.is_empty() {
        return;
    }

    // 2) 候选机器人池：空闲 + 心跳未超时 + 电量非低 + 不在让路冷却窗口内。
    let mut free: Vec<RobotState> = store.available_robots(now, config.heartbeat_timeout_ms);
    free.retain(|r| !r.battery.is_low() && cooldown.get(&r.id).is_none_or(|&until| now >= until));
    if free.is_empty() {
        return; // 无机器人可用，任务保留在 pending 等待下一 tick
    }

    // 3) 依据有效优先级排序（高者先；同优先级早创建先；再按 id 稳定）。
    let mut ordered: Vec<TaskKey> = pending
        .values()
        .map(|t| TaskKey {
            priority: t
                .effective_priority(now.saturating_sub(t.created_at), config.priority_aging_ms),
            created_at: t.created_at,
            id: t.id,
        })
        .collect();
    ordered.sort_by(|a, b| b.cmp(a)); // TaskKey 的 Ord：越大代表优先级越高/越早到

    // 4) 贪心匹配。
    for key in ordered {
        let Some(task) = pending.get(&key.id).cloned() else {
            continue;
        };
        // 任务首个需前往的节点（取货点/充电桩/首航点），用于就近选车。
        let Some(first_node) = task_target(&task) else {
            pending.remove(&key.id); // 非法目标，丢弃
            continue;
        };
        let Some(first_loc) = map.node_location(first_node).copied() else {
            pending.remove(&key.id);
            continue;
        };

        // 选距取货点最近的候选机器人；无可达者本 tick 跳过（保留在 pending）。
        let Some(idx) = nearest_robot(map, &free, &first_loc) else {
            continue;
        };
        let robot_id = free[idx].id;
        let Some(robot_node) = free[idx].location.node else {
            continue;
        };

        // 规划完整行驶路线：取货任务为 机器人→取货点→放货点 合并路径（目的地取放货点），
        // 其余任务为单段直达。任一段不可达则顺延下一 tick。
        let Some((path, final_loc, pickup)) = plan_task_route(pathfinder, map, robot_node, &task)
        else {
            debug!(
                "任务 {} 无法从 {} 规划路线，顺延下一 tick",
                task.id, robot_id
            );
            continue;
        };

        // 落库（机器人 Busy + 任务 Assigned），并把规划路线一并写入供 GUI 绘制。
        match store.assign(robot_id, task.id, final_loc, path.to_vec()) {
            Ok((_r, _t)) => {
                let d = Dispatch {
                    robot: robot_id,
                    task: task.id,
                    path: path.clone(),
                    pickup,
                };
                if dispatch.try_send(d).is_err() {
                    // 下发通道拥塞：回滚本次分配，任务与机器人留待下一 tick 重试，并提前结束本轮。
                    store.unassign(robot_id, task.id);
                    warn!("下发通道拥塞，回滚任务 {}，顺延下一 tick 重试", task.id);
                    break;
                }
                free.swap_remove(idx);
                pending.remove(&key.id);
                info!(
                    "分配: task={} -> robot={} 路径长度={}",
                    task.id,
                    robot_id,
                    path.len()
                );
                if free.is_empty() {
                    break;
                }
            }
            Err(e) => {
                // 分配失败通常因机器人与本地快照不一致（已被占用/离线），本 tick 剔除该候选。
                debug!("分配失败({e})，机器人 {} 本 tick 移出候选", robot_id);
                free.swap_remove(idx);
            }
        }
    }
}

/// 检测并解除死锁：对当前帧机器人快照跑冲突分析，对每个死锁环假释一台有效优先级最低的牺牲者让路。
fn resolve_deadlocks(
    store: &MemoryStore,
    config: &SystemConfig,
    cooldown: &mut HashMap<RobotId, u64>,
    now: u64,
) {
    let robots = store.all_robots();
    let report = conflict::analyze(&robots);
    if report.deadlock_cycles.is_empty() {
        return;
    }

    // 机器人 -> (当前任务, 任务有效优先级)；无任务者按最低优先级对待。
    let mut robot_task: HashMap<RobotId, TaskId> = HashMap::new();
    let mut robot_prio: HashMap<RobotId, TaskPriority> = HashMap::new();
    for r in &robots {
        if let Some(tid) = r.current_task {
            robot_task.insert(r.id, tid);
            let prio = store
                .get_task(tid)
                .map(|t| {
                    t.effective_priority(now.saturating_sub(t.created_at), config.priority_aging_ms)
                })
                .unwrap_or(TaskPriority::Low);
            robot_prio.insert(r.id, prio);
        }
    }

    for cycle in &report.deadlock_cycles {
        let Some(victim) = pick_victim(cycle, &robot_prio) else {
            continue;
        };
        if let Some(&tid) = robot_task.get(&victim) {
            store.parole_robot(victim, tid);
            cooldown.insert(victim, now.saturating_add(config.deadlock_cooldown_ms));
            info!(
                "死锁解除: 假释牺牲者 {} 让路（环规模 {}）",
                victim,
                cycle.len()
            );
        }
    }
}

/// 在死锁环内选牺牲者：有效任务优先级**最低**者（最不值得阻塞他人），平手取 id **最大**者。
fn pick_victim(cycle: &[RobotId], prio: &HashMap<RobotId, TaskPriority>) -> Option<RobotId> {
    cycle.iter().copied().max_by(|a, b| {
        let pa = prio.get(a).copied().unwrap_or(TaskPriority::Low);
        let pb = prio.get(b).copied().unwrap_or(TaskPriority::Low);
        // 优先级越低越应作牺牲者（reverse）；平手时 id 更大者作牺牲者。
        pa.cmp(&pb).reverse().then(a.0.cmp(&b.0))
    })
}

/// 在候选机器人中选出到 `target` 直线距离最近者的下标。
fn nearest_robot(map: &Map, robots: &[RobotState], target: &Location) -> Option<usize> {
    let mut best: Option<(usize, f32)> = None;
    for (i, r) in robots.iter().enumerate() {
        let Some(rn) = r.location.node else { continue };
        let Some(rl) = map.node_location(rn) else {
            continue;
        };
        let d = rl.distance_to(target);
        if best.is_none_or(|(_, bd)| d < bd) {
            best = Some((i, d));
        }
    }
    best.map(|(i, _)| i)
}

/// 规划某任务从机器人当前位置 `from` 出发的完整行驶路线。
/// - 搬运（取货送达）：`from → 取货点 → 放货点` 的合并路径（去重衔接的取货点），
///   返回 `(全程路径, 放货点坐标, Some(取货节点))`；
/// - 其它（回充/巡检）：`from → 目的节点` 单段，返回 `(路径, 终点坐标, None)`。
///
/// 任一段不可达则返回 `None`（本 tick 顺延）。
fn plan_task_route<P: Pathfinder + ?Sized>(
    pf: &P,
    map: &Map,
    from: NodeId,
    task: &Task,
) -> Option<(Path, Location, Option<NodeId>)> {
    match &task.kind {
        TaskKind::Transport { pickup, dropoff } => {
            let pick = pickup.node?;
            let drop = dropoff.node?;
            let mut full = pf.find_path(map, from, pick)?;
            if pick != drop {
                let leg2 = pf.find_path(map, pick, drop)?;
                full.extend(leg2.iter().skip(1).copied());
            }
            let final_loc = map.node_location(drop).copied()?;
            Some((full, final_loc, Some(pick)))
        }
        _ => {
            let goal = task.goal_node()?;
            let path = pf.find_path(map, from, goal)?;
            let loc = map.node_location(goal).copied()?;
            Some((path, loc, None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pick_victim_prefers_lowest_priority_then_largest_id() {
        let r1 = RobotId(1);
        let r2 = RobotId(2);
        let r3 = RobotId(3);
        let mut prio = HashMap::new();
        prio.insert(r1, TaskPriority::Urgent);
        prio.insert(r2, TaskPriority::Low);
        prio.insert(r3, TaskPriority::Low);
        // 环内最低优先级为 r2/r3，平手取 id 最大 -> r3。
        let cycle = [r1, r2, r3];
        assert_eq!(pick_victim(&cycle, &prio), Some(r3));

        // 含高/低混合：应选唯一的低优先级 r2，即使不在末尾。
        let mut p2 = HashMap::new();
        p2.insert(r1, TaskPriority::High);
        p2.insert(r2, TaskPriority::Low);
        p2.insert(r3, TaskPriority::Urgent);
        assert_eq!(pick_victim(&[r1, r2, r3], &p2), Some(r2));

        // 未登记优先级的机器人按 Low 对待，应被选中。
        assert_eq!(pick_victim(&[r1, r2], &HashMap::new()), Some(r2));
        // 空环返回 None。
        assert_eq!(pick_victim(&[], &prio), None);
    }

    #[test]
    fn plan_task_route_stitches_pickup_and_dropoff() {
        use crate::engine::AStarPathfinder;
        use crate::service::build_grid_map;
        // 4x1 一字型网格：节点 0-1-2-3 依次相邻。
        let map = build_grid_map(4, 1);
        let loc = |n: u32| map.node_location(NodeId(n)).copied().unwrap();
        let task = Task::new(
            TaskId(1),
            TaskKind::Transport {
                pickup: loc(1),
                dropoff: loc(3),
            },
            TaskPriority::Normal,
            0,
        );
        let (path, final_loc, pickup) =
            plan_task_route(&AStarPathfinder, &map, NodeId(0), &task).unwrap();
        // 取货节点回传为 Some(1)，目的地为放货点 3。
        assert_eq!(pickup, Some(NodeId(1)));
        assert_eq!(final_loc, loc(3));
        // 合并路径 0->1->2->3，衔接的取货点 1 只出现一次（去重）。
        assert_eq!(
            path.as_slice(),
            &[NodeId(0), NodeId(1), NodeId(2), NodeId(3)]
        );
        assert_eq!(path.iter().filter(|&&n| n == NodeId(1)).count(), 1);
    }
}

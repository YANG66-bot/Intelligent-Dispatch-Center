//! 调度任务领域模型 (Task Domain Model)
//!
//! 定义调度系统里的最小工作单元 [`Task`]：包含业务类型、优先级、生命周期状态机，
//! 以及供调度器排序决策使用的排序键。优先级通过实现 `Ord` 保证在
//! 二叉堆（[`std::collections::BinaryHeap`]）中“高优先级先出”。

use serde::{Deserialize, Serialize};

use super::map::{Location, NodeId};
use super::robot::RobotId;

/// 任务唯一标识。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskId(pub u64);

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TASK-{:06}", self.0)
    }
}

/// 任务优先级。`Reverse` 包装进最小堆或使用 discriminant 排序，
/// 这里显式实现 `Ord`：`Urgent > High > Normal > Low`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum TaskPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Urgent = 3,
}

impl TaskPriority {
    /// 提升一级优先级，已为最高级 (`Urgent`) 则封顶不变。用于老化防饥饿。
    #[inline]
    pub fn bumped(self) -> Self {
        use TaskPriority::*;
        match self {
            Low => Normal,
            Normal => High,
            High | Urgent => Urgent,
        }
    }
}

/// 任务生命周期状态机。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// 已创建，等待被分配
    Pending,
    /// 已分配给某机器人，等待其确认
    Assigned,
    /// 机器人正在执行
    Executing,
    /// 执行完成
    Completed,
    /// 执行失败
    Failed,
}

/// 任务业务类型。使用 `Box` 携带大变体以控制 `TaskKind` 的整体尺寸（缓存友好）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TaskKind {
    /// 搬运：从取货点送到卸货点
    Transport { pickup: Location, dropoff: Location },
    /// 回充：前往充电节点
    Charge { charger: NodeId },
    /// 巡检：依次经过若干航点
    Patrol { waypoints: Box<Vec<NodeId>> },
}

/// 一个完整的调度任务。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub kind: TaskKind,
    pub priority: TaskPriority,
    pub status: TaskStatus,
    /// 被指派执行的机器人（未分配时为 None）
    pub assigned_robot: Option<RobotId>,
    /// 创建时间戳（毫秒），同优先级下遵循先到先服务 (FCFS)
    pub created_at: u64,
    /// 已被回收重试的次数（达到上限后转为失败）。`serde(default)` 保证旧数据兼容。
    #[serde(default)]
    pub retry_count: u8,
    /// 最近一次被分配给机器人的时刻（单调毫秒）；未分配时为 0。供超时回收判据使用。
    #[serde(default)]
    pub assigned_at: u64,
}

impl Task {
    /// 创建一个处于 `Pending` 状态的新任务。
    pub fn new(id: TaskId, kind: TaskKind, priority: TaskPriority, created_at: u64) -> Self {
        Self {
            id,
            kind,
            priority,
            status: TaskStatus::Pending,
            assigned_robot: None,
            created_at,
            retry_count: 0,
            assigned_at: 0,
        }
    }

    /// 老化后的**有效优先级**：每等待满 `aging_ms` 提升一级，封顶 `Urgent`。
    /// `waited_ms` 为已等待毫秒数（通常 `now - created_at`）。`aging_ms` 非正时不老化。
    #[inline]
    pub fn effective_priority(&self, waited_ms: u64, aging_ms: u64) -> TaskPriority {
        if aging_ms == 0 {
            return self.priority;
        }
        let boosts = (waited_ms / aging_ms) as u32;
        let mut p = self.priority;
        for _ in 0..boosts.min(3) {
            p = p.bumped();
        }
        p
    }

    /// 是否为可被调度器参与分配的待处理任务。
    #[inline]
    pub fn is_pending(&self) -> bool {
        matches!(self.status, TaskStatus::Pending)
    }

    /// 任务的“目的地节点”：机器人需前往的第一个节点。
    /// - 搬运：取货点所在节点；
    /// - 回充：充电桩节点；
    /// - 巡检：首个航点。
    #[inline]
    pub fn goal_node(&self) -> Option<NodeId> {
        match &self.kind {
            TaskKind::Transport { pickup, .. } => pickup.node,
            TaskKind::Charge { charger } => Some(*charger),
            TaskKind::Patrol { waypoints } => waypoints.first().copied(),
        }
    }

    /// 任务业务类型的简短中文描述（供 UI 展示）。
    pub fn kind_desc(&self) -> String {
        match &self.kind {
            TaskKind::Transport { pickup, dropoff } => {
                let p = pickup
                    .node
                    .map(|n| n.0.to_string())
                    .unwrap_or_else(|| "?".into());
                let d = dropoff
                    .node
                    .map(|n| n.0.to_string())
                    .unwrap_or_else(|| "?".into());
                format!("搬运 N{p} → N{d}")
            }
            TaskKind::Charge { charger } => format!("回充 N{}", charger.0),
            TaskKind::Patrol { waypoints } => format!("巡检 {} 个航点", waypoints.len()),
        }
    }
}

/// 排序键：先比优先级，再比创建时间（越早越优先）。
/// 供调度器在堆中比较使用，避免直接比较整个 `Task` 结构。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskKey {
    pub priority: TaskPriority,
    pub created_at: u64,
    pub id: TaskId,
}

impl Ord for TaskKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // 优先级高者优先；同优先级下创建早者优先（故 created_at 取反向）
        self.priority
            .cmp(&other.priority)
            .then(other.created_at.cmp(&self.created_at))
    }
}

impl PartialOrd for TaskKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::map::{Location, NodeId};

    fn transport_task(priority: TaskPriority) -> Task {
        let a = Location::at_node(NodeId(1), 0.0, 0.0);
        let b = Location::at_node(NodeId(2), 1.0, 1.0);
        Task::new(
            TaskId(1),
            TaskKind::Transport {
                pickup: a,
                dropoff: b,
            },
            priority,
            0,
        )
    }

    #[test]
    fn effective_priority_ages_and_caps() {
        let t = transport_task(TaskPriority::Low);
        // aging_ms=1000：等待 0ms 不变，等待 1000~1999ms 升一级，>=3000ms 封顶 Urgent。
        assert_eq!(t.effective_priority(0, 1000), TaskPriority::Low);
        assert_eq!(t.effective_priority(1_500, 1000), TaskPriority::Normal);
        assert_eq!(t.effective_priority(2_500, 1000), TaskPriority::High);
        assert_eq!(t.effective_priority(9_000, 1000), TaskPriority::Urgent);
        // 已是 Urgent 时封顶不变。
        let u = transport_task(TaskPriority::Urgent);
        assert_eq!(u.effective_priority(99_000, 1000), TaskPriority::Urgent);
        // aging_ms=0 关闭老化。
        assert_eq!(t.effective_priority(99_000, 0), TaskPriority::Low);
    }

    #[test]
    fn task_key_orders_by_priority_then_fcfs() {
        let hi = TaskKey {
            priority: TaskPriority::High,
            created_at: 100,
            id: TaskId(1),
        };
        let lo = TaskKey {
            priority: TaskPriority::Low,
            created_at: 0,
            id: TaskId(2),
        };
        assert!(hi > lo, "高优先级排序键应更大（先出）");
        let early = TaskKey {
            priority: TaskPriority::Normal,
            created_at: 10,
            id: TaskId(3),
        };
        let late = TaskKey {
            priority: TaskPriority::Normal,
            created_at: 20,
            id: TaskId(4),
        };
        assert!(early > late, "同优先级下早创建者排序键应更大（FCFS）");
    }
}

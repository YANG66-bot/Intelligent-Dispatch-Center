//! 调度核心引擎 (Scheduling Engine)
//!
//! 本层是系统的“大脑”，由事件驱动的 [`scheduler::Scheduler`] 与可插拔的
//! [`pathfinding::Pathfinder`] 算法组成。引擎只依赖领域模型，不感知网络协议与存储实现细节，
//! 通过 channel 与外部解耦，便于替换调度策略或路径算法。

pub mod conflict;
pub mod pathfinding;
pub mod scheduler;

use crate::domain::{NodeId, Path, RobotId, Task, TaskId};

pub use conflict::ConflictReport;
pub use pathfinding::{AStarPathfinder, Pathfinder};
pub use scheduler::{Scheduler, SchedulerHandle};

/// 流入调度器的事件。通过有界 channel 传递，实现“状态上报 / 任务提交”与“调度决策”的解耦。
#[derive(Debug, Clone)]
pub enum SchedulerEvent {
    /// 有新任务被提交，需要进入待分配队列
    TaskSubmitted(Task),
    /// 某机器人完成了任务，可释放并触发再调度
    TaskCompleted { robot: RobotId, task: TaskId },
    /// 优雅停机
    Shutdown,
}

/// 调度器产出的下发指令（领域级，尚未编码为具体协议帧）。
///
/// 由 transport 层的“派发泵”消费，转成对应机器人的下行帧。
#[derive(Debug, Clone)]
pub struct Dispatch {
    pub robot: RobotId,
    pub task: TaskId,
    /// 规划出的行驶路径（节点序列；取货任务为 起点→取货点→放货点 合并）
    pub path: Path,
    /// 取货节点（供机器人到达时切换载货态；非取货任务为 None）
    pub pickup: Option<NodeId>,
}

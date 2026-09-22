//! 领域模型层 (Core Domain Models)
//!
//! 本模块是整个调度系统的“通用语言”，不依赖任何其它分层模块，
//! 仅包含纯粹的数据结构与领域行为，保证可被 engine / storage / transport 自由复用。
//!
//! 设计原则：
//! - 核心标识符使用 newtype 包装（[`RobotId`]、[`TaskId`]、[`NodeId`]），在编译期防止 id 混用。
//! - 小体积、可复制的状态优先实现 `Copy`，避免不必要的堆分配与 `Clone` 开销。
//! - 所有模型均实现 `serde` 序列化，以便跨网络/存储传输。

pub mod map;
pub mod robot;
pub mod task;

pub use map::{Edge, Location, Map, NodeId, Path};
pub use robot::{Battery, Heartbeat, RobotId, RobotState, RobotStatus, TimestampMs};
pub use task::{Task, TaskId, TaskKey, TaskKind, TaskPriority, TaskStatus};

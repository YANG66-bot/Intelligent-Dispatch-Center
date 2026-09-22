//! 高性能状态存储层 (State Store)
//!
//! 提供调度系统运行期的“事实来源”(source of truth)：机器人实时状态与任务状态。
//! 本层不感知调度算法与网络协议，仅暴露高并发安全的读写接口，供 engine / transport 复用。

pub mod memory_store;

pub use memory_store::{MemoryStore, ReclaimOutcome, StoreError};

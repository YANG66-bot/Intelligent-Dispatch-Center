//! 网络传输与协议接入层 (Transport & Protocols)
//!
//! 负责与机器人建立长连接、解析上报帧、下发调度指令。本层把“线上字节”与“领域事件/指令”
//! 双向翻译，并通过 [`SchedulerHandle`] 与 [`Dispatch`] 通道与调度引擎解耦。

pub mod codec;
pub mod server;

pub use codec::{BincodeCodec, ClientMsg, ServerMsg};
pub use server::{ConnectionRegistry, TransportServer, dispatch_pump};

use crate::engine::SchedulerHandle;

/// transport 层错误。
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),
    #[error("编解码错误: {0}")]
    Codec(String),
    #[error("连接已关闭")]
    Closed,
}

/// 供上层复用的调度句柄别名，强调 transport 只依赖引擎的事件入口。
pub type EngineHandle = SchedulerHandle;

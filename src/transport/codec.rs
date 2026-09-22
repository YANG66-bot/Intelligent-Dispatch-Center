//! 协议编解码 (Codec)
//!
//! 采用“4 字节大端长度前缀 + bincode 二进制负载”的紧凑帧格式：
//! - 长度前缀解决 TCP 粘包/半包；
//! - bincode 相较 JSON 序列化更小更快，适合高频心跳上报。
//!
//! [`BincodeCodec`] 以“解码类型 `In` / 编码类型 `Out`”两个泛型参数统一服务端与客户端，
//! 服务端为 `BincodeCodec<ClientMsg, ServerMsg>`，客户端镜像为 `BincodeCodec<ServerMsg, ClientMsg>`。

use std::marker::PhantomData;

use bytes::{Buf, BytesMut};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

use crate::domain::{Battery, Heartbeat, Location, NodeId, RobotId, TaskId};

/// 机器人 -> 服务端 的上行消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMsg {
    /// 上线注册
    Register {
        id: RobotId,
        location: Location,
        battery: Battery,
    },
    /// 周期心跳 / 状态上报
    Heartbeat(Heartbeat),
    /// 任务执行完成上报
    TaskDone { robot: RobotId, task: TaskId },
}

/// 服务端 -> 机器人 的下行消息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMsg {
    /// 注册成功确认
    RegisterAck { robot: RobotId },
    /// 下发任务及其行驶路径
    TaskAssign {
        task: TaskId,
        /// 节点序列，机器人据此逐节点行驶（取货任务为 起点→取货点→放货点 的合并路径）
        path: Vec<NodeId>,
        /// 取货节点：机器人驶至该节点时置为载货（非取货任务时为 None）
        #[serde(default)]
        pickup: Option<NodeId>,
    },
}

/// 单帧最大长度保护，防止畸形长度前缀导致 OOM（默认 1 MiB）。
const MAX_FRAME_LEN: usize = 1024 * 1024;
/// 长度前缀字节数。
const LEN_PREFIX: usize = 4;

/// 通用长度前缀 + bincode 编解码器。
pub struct BincodeCodec<In, Out> {
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<In, Out> Default for BincodeCodec<In, Out> {
    fn default() -> Self {
        Self::new()
    }
}

impl<In, Out> BincodeCodec<In, Out> {
    pub fn new() -> Self {
        Self {
            _in: PhantomData,
            _out: PhantomData,
        }
    }
}

impl<In: DeserializeOwned, Out> Decoder for BincodeCodec<In, Out> {
    type Item = In;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 不足长度前缀：预留并等待更多字节
        if src.len() < LEN_PREFIX {
            src.reserve(LEN_PREFIX - src.len());
            return Ok(None);
        }
        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        if len > MAX_FRAME_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame too large",
            ));
        }
        // 半包：整帧未到达则预留空间继续等待
        if src.len() < LEN_PREFIX + len {
            src.reserve(LEN_PREFIX + len - src.len());
            return Ok(None);
        }
        src.advance(LEN_PREFIX);
        let body = src.split_to(len);
        let msg = bincode::deserialize::<In>(&body)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(Some(msg))
    }
}

impl<In, Out: Serialize> Encoder<Out> for BincodeCodec<In, Out> {
    type Error = std::io::Error;

    fn encode(&mut self, item: Out, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let body = bincode::serialize(&item)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        dst.reserve(LEN_PREFIX + body.len());
        dst.extend_from_slice(&(body.len() as u32).to_be_bytes());
        dst.extend_from_slice(&body);
        Ok(())
    }
}

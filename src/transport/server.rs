//! TCP 服务端 (Async Server)
//!
//! # 职责
//! - 用 [`tokio::net::TcpListener`] 接受机器人长连接；
//! - 每条连接用 [`tokio_util::codec::Framed`] 组合 [`BincodeCodec`] 做帧化读写；
//! - 上行：`Register` 注册进 [`MemoryStore`]，`Heartbeat` 刷新状态，`TaskDone` 通知调度器；
//! - 下行：为每个连接维护一条 `flume` 下行通道，注册进 [`ConnectionRegistry`]，
//!   由 `dispatch_pump` 把调度器产出的 [`Dispatch`] 路由到对应连接。
//!
//! # 并发与解耦
//! 读循环运行于连接任务，写循环独立 `spawn`；两者以“连接内 channel”衔接，
//! 使“下发”与“上报”互不阻塞。连接注册表用 `DashMap`，读多写少且分片加锁，适配上千连接规模。

use std::sync::Arc;

use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::config::SystemConfig;
use crate::domain::{RobotId, RobotState};
use crate::engine::{Dispatch, SchedulerHandle};
use crate::storage::MemoryStore;
use crate::transport::codec::{BincodeCodec, ClientMsg, ServerMsg};

/// 机器人下行连接注册表：`RobotId -> 下发发送端`。
pub type ConnectionRegistry = DashMap<RobotId, flume::Sender<ServerMsg>>;

/// 服务端句柄，持有连接注册表，供外部查询在线连接或注入下发。
#[derive(Clone)]
pub struct ServerHandle {
    registry: Arc<ConnectionRegistry>,
}

impl ServerHandle {
    /// 当前保活的机器人下行连接数。
    pub fn connection_count(&self) -> usize {
        self.registry.len()
    }
}

/// 基于 store/engine/registry 组装出的传输服务端。
pub struct TransportServer {
    store: MemoryStore,
    engine: SchedulerHandle,
    registry: Arc<ConnectionRegistry>,
    config: SystemConfig,
}

impl TransportServer {
    pub fn new(
        store: MemoryStore,
        engine: SchedulerHandle,
        registry: Arc<ConnectionRegistry>,
        config: SystemConfig,
    ) -> Self {
        Self {
            store,
            engine,
            registry,
            config,
        }
    }

    /// 返回一个可克隆的服务端句柄。
    pub fn handle(&self) -> ServerHandle {
        ServerHandle {
            registry: self.registry.clone(),
        }
    }

    /// 接受循环：为每条连接 spawn 独立任务。应通过 `tokio::spawn(server.run(listener))` 启动。
    pub async fn run(self, listener: tokio::net::TcpListener) {
        let Self {
            store,
            engine,
            registry,
            config,
        } = self;
        info!("TCP 服务端已启动，监听 {}", config.listen_addr);
        loop {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    let store = store.clone();
                    let engine = engine.clone();
                    let registry = registry.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, store, engine, registry).await {
                            debug!("连接 {addr} 结束: {e}");
                        }
                    });
                }
                Err(e) => warn!("accept 失败: {e}"),
            }
        }
    }
}

/// 处理单条机器人长连接。
async fn handle_connection(
    socket: TcpStream,
    store: MemoryStore,
    engine: SchedulerHandle,
    registry: Arc<ConnectionRegistry>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // 关闭 Nagle，降低小帧（心跳/指令）传输延迟
    let _ = socket.set_nodelay(true);
    let peer = socket.peer_addr()?;

    let codec = BincodeCodec::<ClientMsg, ServerMsg>::new();
    // 注意：SinkExt::split() 返回 (SplitSink, SplitStream)，sink 在前。
    let (mut writer, mut reader) = Framed::new(socket, codec).split();

    // 首帧必须是注册
    let robot_id = loop {
        match reader.next().await {
            Some(Ok(ClientMsg::Register {
                id,
                location,
                battery,
            })) => {
                let state =
                    RobotState::onboard(id, location, battery, crate::config::monotonic_ms());
                store.register_robot(state);
                info!("机器人 {id} 注册成功 (peer={peer})");
                break id;
            }
            Some(Ok(_)) => {
                warn!("{peer} 首帧非 Register，忽略并继续等待");
            }
            Some(Err(e)) => return Err(e.into()),
            None => return Err("连接在注册前关闭".into()),
        }
    };

    // 建立下行通道并注册
    let (down_tx, down_rx) = flume::unbounded::<ServerMsg>();
    registry.insert(robot_id, down_tx.clone());
    // 注册确认走下行通道，保证与其它下行帧同序
    let _ = down_tx.send(ServerMsg::RegisterAck { robot: robot_id });

    // 写任务：把下行消息刷入 socket；下行通道关闭后自然退出
    let writer_task = tokio::spawn(async move {
        while let Ok(msg) = down_rx.recv_async().await {
            if let Err(e) = writer.send(msg).await {
                debug!("{robot_id} 下行写失败: {e}");
                break;
            }
        }
        // 尝试关闭写半区
        let _ = writer.close().await;
    });

    // 读循环：处理上行帧
    while let Some(item) = reader.next().await {
        match item {
            Ok(ClientMsg::Heartbeat(hb)) => {
                if let Err(e) = store.apply_heartbeat(hb) {
                    debug!("心跳应用到未知机器人: {e}");
                }
            }
            Ok(ClientMsg::TaskDone { robot, task }) => {
                info!("机器人 {robot} 完成任务 {task}");
                let _ = engine.report_complete(robot, task);
            }
            Ok(ClientMsg::Register { .. }) => {
                debug!("{robot_id} 重复注册，忽略")
            }
            Err(e) => {
                debug!("{robot_id} 上行解码错误: {e}");
                break;
            }
        }
    }

    // 清理：注销连接；本地 down_tx 与写任务句柄随后释放
    registry.remove(&robot_id);
    drop(down_tx);
    let _ = writer_task.await;
    Ok(())
}

/// 派发泵：消费调度器产出的 [`Dispatch`]，编码为下行帧并路由到对应机器人连接。
///
/// 该任务把“调度决策”与“网络下发”彻底解耦——即便某机器人暂时掉线（注册表无其通道），
/// 也只是丢弃其下发，不影响调度器主循环。
pub async fn dispatch_pump(rx: flume::Receiver<Dispatch>, registry: Arc<ConnectionRegistry>) {
    while let Ok(d) = rx.recv_async().await {
        let msg = ServerMsg::TaskAssign {
            task: d.task,
            path: d.path.into_vec(),
            pickup: d.pickup,
        };
        match registry.get(&d.robot) {
            Some(sender) => {
                // unbounded 通道，try_send 实际不会因满而失败
                let _ = sender.send(msg);
            }
            None => warn!("机器人 {} 无活动连接，下发丢弃", d.robot),
        }
    }
    debug!("派发泵退出（调度器已停止下发）");
}

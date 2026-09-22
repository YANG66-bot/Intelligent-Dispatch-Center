//! 仿真器 (Simulator) —— 虚拟机器人客户端 + 任务流发生器
//!
//! 这些是**演示/压测**专用代码：以真实 TCP 客户端身份接入 [`crate::transport`]，
//! 模拟“注册 → 周期心跳 → 接收任务 → 行驶 → 回报完成”的行为，并持续产生随机搬运任务。
//! GUI 面板与无头 CLI 共用本模块，保证两端行为一致。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, atomic::AtomicBool};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rand::Rng;
use tokio::net::TcpStream;
use tokio::runtime::Handle;
use tokio_util::codec::Framed;

use crate::config::monotonic_ms;
use crate::domain::{
    Battery, Heartbeat, Location, Map, NodeId, RobotId, RobotStatus, Task, TaskId, TaskKind,
    TaskPriority,
};
use crate::engine::{AStarPathfinder, Pathfinder, SchedulerHandle};
use crate::service::charger_nodes;
use crate::transport::codec::{BincodeCodec, ClientMsg, ServerMsg};

/// 拆分后的写端类型（供 `drive` 辅助函数签名使用）。
type Writer =
    futures::stream::SplitSink<Framed<TcpStream, BincodeCodec<ServerMsg, ClientMsg>>, ClientMsg>;

/// 机器人在两节点之间的“行驶”速度（每节点毫秒），用于仿真执行耗时。
const MS_PER_NODE: u64 = 160;

/// 充电时每心跳的回升幅度（%）。
const CHARGE_STEP: u8 = 10;

/// 抵达取货点后模拟装卸的停留拍数（每拍 `MS_PER_NODE`）。
const LOAD_DWELL: u64 = 2;

/// 启动 `count` 台虚拟机器人，返回其任务句柄（用于停止时 abort）。
/// 机器人 id 从 `1` 递增。
pub fn spawn_robot_fleet(
    rt: &Handle,
    map: Arc<Map>,
    addr: std::net::SocketAddr,
    count: u64,
) -> Vec<tokio::task::JoinHandle<()>> {
    (1..=count)
        .map(|i| {
            let map = map.clone();
            rt.spawn(simulate_robot(RobotId(i), map, addr))
        })
        .collect()
}

/// 沿节点路径逐节点行驶：每到一站更新 `loc`、按节点耗电并上报一次心跳（每站 `MS_PER_NODE`），
/// 使后端轨迹与 GUI 动画看到连续移动而非瞬移。若 `pickup` 命中某站，则模拟装卸停留
/// 若干拍并将后续心跳标为载货。发送失败返回 false（连接已断）。
#[allow(clippy::too_many_arguments)]
async fn drive(
    writer: &mut Writer,
    map: &Map,
    id: RobotId,
    loc: &mut Location,
    battery: &mut Battery,
    path: &[NodeId],
    status: RobotStatus,
    pickup: Option<NodeId>,
) -> bool {
    let mut carrying = false;
    for &n in path {
        if let Some(l) = map.node_location(n) {
            *loc = *l;
        }
        *battery = Battery(battery.0.saturating_sub(1));
        let msg = ClientMsg::Heartbeat(Heartbeat {
            id,
            battery: *battery,
            location: *loc,
            status,
            timestamp: monotonic_ms(),
            carrying,
        });
        if writer.send(msg).await.is_err() {
            return false;
        }
        sleep_or_stop(Duration::from_millis(MS_PER_NODE)).await;
        // 抵达取货点：停留若干拍模拟装卸，并切换为载货态（此后心跳携带 carrying=true）。
        if !carrying && Some(n) == pickup {
            carrying = true;
            for _ in 0..LOAD_DWELL {
                let msg = ClientMsg::Heartbeat(Heartbeat {
                    id,
                    battery: *battery,
                    location: *loc,
                    status,
                    timestamp: monotonic_ms(),
                    carrying,
                });
                if writer.send(msg).await.is_err() {
                    return false;
                }
                sleep_or_stop(Duration::from_millis(MS_PER_NODE)).await;
            }
        }
    }
    true
}

/// 单台虚拟机器人的完整生命周期：连接、注册、心跳、执行任务。
/// 该 future 会一直运行，直到其 [`tokio::task::JoinHandle`] 被 abort。
async fn simulate_robot(id: RobotId, map: Arc<Map>, addr: std::net::SocketAddr) {
    let Ok(stream) = TcpStream::connect(addr).await else {
        return;
    };
    let _ = stream.set_nodelay(true);

    let codec = BincodeCodec::<ServerMsg, ClientMsg>::new();
    // SinkExt::split() 返回 (SplitSink, SplitStream)，sink 在前
    let (mut writer, mut reader) = Framed::new(stream, codec).split();

    // 初始落点与电量（把非 Send 的 ThreadRng 约束在块内，避免跨 await 持有）
    let (mut battery, mut loc) = {
        let mut rng = rand::rng();
        let nodes = map.all_node_locations();
        let (_, l) = nodes[rng.random_range(0..nodes.len())];
        (Battery::new(rng.random_range(60..=100)), l)
    };

    // 充电桩位置（取路网角点），供低电量时驶回回充。
    let chargers: Vec<Location> = charger_nodes(&map)
        .iter()
        .filter_map(|n| map.node_location(*n).copied())
        .collect();
    // 本地充电状态机：为 true 时正在驶回/停靠充电桩回充，不接普通任务。
    let mut charging = false;

    if writer
        .send(ClientMsg::Register {
            id,
            location: loc,
            battery,
        })
        .await
        .is_err()
    {
        return;
    }

    let mut hb = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = hb.tick() => {
                if charging {
                    // 在站充电：回升电量，充满后恢复空闲。
                    battery = Battery((battery.0 + CHARGE_STEP).min(100));
                    if battery.is_full() {
                        charging = false;
                    }
                    let msg = ClientMsg::Heartbeat(Heartbeat {
                        id,
                        battery,
                        location: loc,
                        status: RobotStatus::Charging,
                        timestamp: monotonic_ms(),
                        carrying: false,
                    });
                    if writer.send(msg).await.is_err() {
                        break;
                    }
                } else if battery.is_low() {
                    // 低电量：沿 A* 路径逐节点驶向最近充电桩（而非瞬移），到站后转入在站充电。
                    charging = true;
                    let mut alive = true;
                    if let Some(c) = nearest_charger(&chargers, loc) {
                        let path = match (loc.node, c.node) {
                            (Some(from), Some(to)) => AStarPathfinder.find_path(&map, from, to),
                            _ => None,
                        };
                        match path {
                            Some(p) => {
                                alive = drive(
                                    &mut writer,
                                    &map,
                                    id,
                                    &mut loc,
                                    &mut battery,
                                    &p,
                                    RobotStatus::Charging,
                                    None,
                                )
                                .await;
                            }
                            // 无可达路径：兜底直接落位到充电桩。
                            None => {
                                loc = c;
                            }
                        }
                    }
                    if !alive {
                        break;
                    }
                    let msg = ClientMsg::Heartbeat(Heartbeat {
                        id,
                        battery,
                        location: loc,
                        status: RobotStatus::Charging,
                        timestamp: monotonic_ms(),
                        carrying: false,
                    });
                    if writer.send(msg).await.is_err() {
                        break;
                    }
                } else {
                    let msg = ClientMsg::Heartbeat(Heartbeat {
                        id,
                        battery,
                        location: loc,
                        status: RobotStatus::Idle,
                        timestamp: monotonic_ms(),
                        carrying: false,
                    });
                    if writer.send(msg).await.is_err() {
                        break;
                    }
                }
            }
            incoming = reader.next() => {
                match incoming {
                    Some(Ok(ServerMsg::RegisterAck { .. })) => {}
                    Some(Ok(ServerMsg::TaskAssign { task, path, pickup })) => {
                        // 沿路径逐节点行驶（每站一次 Busy 心跳）；驶至取货点时切换载货态，
                        // 使轨迹连续且可见“取货前空载 / 取货后载货”。
                        if !drive(
                            &mut writer,
                            &map,
                            id,
                            &mut loc,
                            &mut battery,
                            &path,
                            RobotStatus::Busy,
                            pickup,
                        )
                        .await
                        {
                            break;
                        }
                        if writer.send(ClientMsg::TaskDone { robot: id, task }).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(_)) | None => break,
                }
            }
        }
    }
}

/// 从充电桩集合中选出距 `from` 直线距离最近者。
fn nearest_charger(chargers: &[Location], from: Location) -> Option<Location> {
    let mut best: Option<(f32, Location)> = None;
    for c in chargers {
        let d = c.distance_to(&from);
        if best.is_none_or(|(bd, _)| d < bd) {
            best = Some((d, *c));
        }
    }
    best.map(|(_, l)| l)
}

/// 可被取消的 sleep：与一个永不结束的未来竞争，使整任务在被 abort 时能立即中断。
async fn sleep_or_stop(dur: Duration) {
    tokio::time::sleep(dur).await;
}

/// 持续产生随机搬运任务流。`running` 置 false 后自然退出。
pub fn spawn_task_stream(
    rt: &Handle,
    handle: SchedulerHandle,
    map: Arc<Map>,
    period_ms: u64,
    running: Arc<AtomicBool>,
    seq: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    rt.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(period_ms));
        while running.load(Ordering::Relaxed) {
            ticker.tick().await;
            let task = make_random_task(&map, &seq);
            if handle.submit_task(task).is_err() {
                break;
            }
        }
    })
}

/// 生成一个随机起终点、随机优先级的搬运任务。
pub fn make_random_task(map: &Map, seq: &AtomicU64) -> Task {
    let mut rng = rand::rng();
    let id = TaskId(seq.fetch_add(1, Ordering::Relaxed));
    let nodes = map.all_node_locations();
    let (_, pa) = nodes[rng.random_range(0..nodes.len())];
    let (_, pb) = nodes[rng.random_range(0..nodes.len())];
    let priority = match rng.random_range(0..4) {
        0 => TaskPriority::Low,
        1 => TaskPriority::Normal,
        2 => TaskPriority::High,
        _ => TaskPriority::Urgent,
    };
    Task::new(
        id,
        TaskKind::Transport {
            pickup: pa,
            dropoff: pb,
        },
        priority,
        monotonic_ms(),
    )
}

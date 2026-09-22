//! 服务装配层 (Service Bootstrap)
//!
//! 把散落在各层的组件“接线”成一个可运行的调度后端，并把运行期共享状态
//! （[`MemoryStore`] 快照、[`Map`] 拓扑、调度句柄、连接句柄、监听地址）统一暴露，
//! 供无头演示与 GUI 面板复用——二者只是“读取同一份事实来源”的不同前端。
//!
//! 启动的后台任务（须在 tokio 运行时上下文中调用 [`Service::start`]）：
//! - 调度器主循环 `Scheduler::run`
//! - TCP 接受循环 `TransportServer::run`
//! - 下发路由泵 `dispatch_pump`

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::config::SystemConfig;
use crate::domain::{Edge, Location, Map, NodeId};
use crate::engine::{AStarPathfinder, Scheduler, SchedulerHandle};
use crate::metrics::{MetricsHistory, run_collector};
use crate::storage::MemoryStore;
use crate::transport::server::{ConnectionRegistry, ServerHandle, TransportServer, dispatch_pump};

/// 指标采样周期（毫秒）。
const METRICS_INTERVAL_MS: u64 = 500;

/// 装配完成的调度服务，持有所有可共享的句柄。Clone 廉价（内部均为 Arc / 通道句柄）。
#[derive(Clone)]
pub struct Service {
    pub store: MemoryStore,
    pub map: Arc<Map>,
    pub handle: SchedulerHandle,
    pub server: ServerHandle,
    pub metrics: MetricsHistory,
    pub addr: SocketAddr,
}

impl Service {
    /// 在**当前 tokio 运行时**上装配并启动后台服务。
    pub async fn start(config: SystemConfig) -> anyhow::Result<Self> {
        // 1) 拓扑路网 + 内存状态存储（共享的“事实来源”）
        let map = Arc::new(build_grid_map(12, 7));
        let store = MemoryStore::new();

        // 2) 调度器（内部创建事件 / 下发通道）
        let (scheduler, handle, dispatch_rx) = Scheduler::new(
            store.clone(),
            map.clone(),
            AStarPathfinder,
            config.clone(),
            config.event_channel_capacity,
        );

        // 3) TCP 监听（:0 表示由系统分配端口）
        let listener = TcpListener::bind(&config.listen_addr).await?;
        let addr = listener.local_addr()?;

        // 4) 传输服务端 + 连接注册表
        let registry = Arc::new(ConnectionRegistry::new());
        let server = TransportServer::new(
            store.clone(),
            handle.clone(),
            registry.clone(),
            config.clone(),
        );
        let server_handle = server.handle();

        // 5) 拉起后台任务
        let metrics = MetricsHistory::default();
        tokio::spawn(scheduler.run());
        tokio::spawn(server.run(listener));
        tokio::spawn(dispatch_pump(dispatch_rx, registry));
        tokio::spawn(run_collector(
            store.clone(),
            metrics.clone(),
            METRICS_INTERVAL_MS,
        ));

        Ok(Self {
            store,
            map,
            handle,
            server: server_handle,
            metrics,
            addr,
        })
    }

    /// 触发调度器优雅停机。
    pub fn shutdown(&self) {
        self.handle.shutdown();
    }
}

/// 构造 `cols × rows` 的网格拓扑路网：相邻节点双向连边，代价 1.0，坐标为 (列, 行)。
pub fn build_grid_map(cols: u32, rows: u32) -> Map {
    let mut map = Map::new();
    let node = |x: u32, y: u32| NodeId(y * cols + x);
    let loc = |x: u32, y: u32| Location::at_node(node(x, y), x as f32, y as f32);

    for x in 0..cols {
        for y in 0..rows {
            map.add_node(node(x, y), loc(x, y));
            if x + 1 < cols {
                map.add_edge(Edge {
                    from: node(x, y),
                    to: node(x + 1, y),
                    cost: 1.0,
                });
                map.add_edge(Edge {
                    from: node(x + 1, y),
                    to: node(x, y),
                    cost: 1.0,
                });
            }
            if y + 1 < rows {
                map.add_edge(Edge {
                    from: node(x, y),
                    to: node(x, y + 1),
                    cost: 1.0,
                });
                map.add_edge(Edge {
                    from: node(x, y + 1),
                    to: node(x, y),
                    cost: 1.0,
                });
            }
        }
    }
    map
}

/// 推导充电桩节点：取路网包围盒的四个角点（存在几个返回几个）。
/// 供仿真器（低电量回充目的地）与 GUI（地图标记）共用，保证两端一致。
pub fn charger_nodes(map: &Map) -> Vec<NodeId> {
    let locs = map.all_node_locations();
    if locs.is_empty() {
        return Vec::new();
    }
    let (mut min_x, mut min_y, mut max_x, mut max_y) = (
        f32::INFINITY,
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::NEG_INFINITY,
    );
    for (_, l) in &locs {
        min_x = min_x.min(l.x);
        min_y = min_y.min(l.y);
        max_x = max_x.max(l.x);
        max_y = max_y.max(l.y);
    }
    let corners = [
        (min_x, min_y),
        (max_x, min_y),
        (min_x, max_y),
        (max_x, max_y),
    ];
    let mut out = Vec::new();
    for (cx, cy) in corners {
        if let Some((id, _)) = locs
            .iter()
            .find(|(_, l)| (l.x - cx).abs() < 0.01 && (l.y - cy).abs() < 0.01)
            && !out.contains(id)
        {
            out.push(*id);
        }
    }
    out
}

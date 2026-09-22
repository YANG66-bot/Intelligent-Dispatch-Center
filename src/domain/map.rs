//! 地图与路网模型 (Map & Road Network)
//!
//! 采用拓扑图（有向带权图）建模仓库/车间路网，为路径规划提供数据基础。
//! 邻接表使用 [`smallvec::SmallVec`]，可使度数较小的节点（路网点常见度数 ≤ 4）
//! 完全分配在栈上，避免海量 `Vec` 堆分配，显著提升缓存局部性与遍历性能。

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::HashMap;

/// 路网节点唯一标识。使用 `u32` 足够表达百万级栅格/拓扑节点。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u32);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "N{}", self.0)
    }
}

/// 二维物理坐标。`Copy` 类型，按值传递零开销。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Location {
    /// 横坐标（米）
    pub x: f32,
    /// 纵坐标（米）
    pub y: f32,
    /// 该坐标所归属的路网节点（可能为空，表示机器人脱离了拓扑约束）
    pub node: Option<NodeId>,
}

impl Location {
    /// 构造一个位于指定节点上的坐标。
    pub fn at_node(node: NodeId, x: f32, y: f32) -> Self {
        Self {
            x,
            y,
            node: Some(node),
        }
    }

    /// 欧氏距离（用于 A* 启发函数）。
    pub fn distance_to(&self, other: &Location) -> f32 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }
}

/// 有向边：从 `from` 指向 `to`，`cost` 为通行代价（可为距离或预计耗时）。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub from: NodeId,
    pub to: NodeId,
    /// 通行代价（越小越优）
    pub cost: f32,
}

/// 路径结果：节点序列。使用 `SmallVec` 内联 8 个节点，覆盖大多数短途路径，避免堆分配。
pub type Path = SmallVec<[NodeId; 8]>;

/// 拓扑路网。内部以 `HashMap` 存储节点坐标、以邻接表存储连接关系。
///
/// 说明：路网在系统启动时构建、运行期通常只读，因此这里使用普通不可变结构即可，
/// 读多写少场景下由上层用 `Arc` 共享，天然线程安全、无锁读取。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Map {
    /// 节点 -> 坐标
    nodes: HashMap<NodeId, Location>,
    /// 节点 -> 出边集合（邻接表）
    adjacency: HashMap<NodeId, SmallVec<[Edge; 4]>>,
}

impl Map {
    /// 创建空路网。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册/更新一个节点坐标。
    pub fn add_node(&mut self, id: NodeId, loc: Location) {
        self.nodes.entry(id).or_insert(loc);
    }

    /// 添加一条有向边（同时确保端点节点存在）。
    pub fn add_edge(&mut self, edge: Edge) {
        self.adjacency.entry(edge.from).or_default().push(edge);
        // 保证反向可达性登记端点，避免查询节点坐标时缺失
        self.adjacency.entry(edge.to).or_default();
    }

    /// 查询节点坐标。
    #[inline]
    pub fn node_location(&self, id: NodeId) -> Option<&Location> {
        self.nodes.get(&id)
    }

    /// 节点总数。
    #[inline]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// 获取某节点的出边列表（借用，零拷贝）。
    #[inline]
    pub fn edges_from(&self, id: NodeId) -> Option<&SmallVec<[Edge; 4]>> {
        self.adjacency.get(&id)
    }

    /// 导出所有节点坐标（供 GUI 绘制路网）。
    pub fn all_node_locations(&self) -> Vec<(NodeId, Location)> {
        self.nodes.iter().map(|(k, v)| (*k, *v)).collect()
    }

    /// 导出所有有向边（供 GUI 绘制连线）。
    pub fn all_edges(&self) -> Vec<Edge> {
        self.adjacency
            .values()
            .flat_map(|es| es.iter().copied())
            .collect()
    }

    /// 路网坐标包围盒 (min_x, min_y, max_x, max_y)，供 GUI 做等比缩放到画布。
    pub fn bounds(&self) -> Option<(f32, f32, f32, f32)> {
        let mut it = self.nodes.values();
        let first = it.next()?;
        let (mut min_x, mut min_y, mut max_x, mut max_y) = (first.x, first.y, first.x, first.y);
        for l in it {
            min_x = min_x.min(l.x);
            min_y = min_y.min(l.y);
            max_x = max_x.max(l.x);
            max_y = max_y.max(l.y);
        }
        Some((min_x, min_y, max_x, max_y))
    }
}

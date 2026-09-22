//! 路径规划 (Pathfinding)
//!
//! 通过 [`Pathfinder`] trait 抽象算法，使调度引擎可在不改动业务代码的前提下切换
//! A*、Dijkstra、JPS 等实现（面向扩展开放）。此处内置一个基于二叉堆的 A* 实现作为参考骨架。

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::domain::{Map, NodeId, Path};

/// 路径规划算法抽象。实现者以拓扑图为输入，输出从 `start` 到 `goal` 的节点序列。
pub trait Pathfinder: Send + Sync {
    fn find_path(&self, map: &Map, start: NodeId, goal: NodeId) -> Option<Path>;
}

/// 对 `f32` 包装以提供 `Ord`，用于放入 [`BinaryHeap`]。
/// 这里假设代价为非 NaN，故用 `partial_cmp` 兜底为 `Equal`。
#[derive(Debug, Clone, Copy, PartialEq)]
struct Cost(f32);

impl Eq for Cost {}

impl Ord for Cost {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for Cost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// 开放列表元素：`estimate = g + h`。使用 [`Reverse`] 使其在最小堆中按估计总代价升序弹出。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OpenNode {
    estimate: Cost,
    node: NodeId,
}

/// A* 寻路器：使用欧氏距离作为启发函数 `h`。
///
/// 该启发函数可采纳（admissible，不高估真实代价），因此 A* 能找到最优路径。
pub struct AStarPathfinder;

impl Pathfinder for AStarPathfinder {
    fn find_path(&self, map: &Map, start: NodeId, goal: NodeId) -> Option<Path> {
        if start == goal {
            let mut p = Path::new();
            p.push(start);
            return Some(p);
        }
        let goal_loc = map.node_location(goal)?;

        // g_score[n] = 起点到 n 的已知最优代价
        let mut g_score: HashMap<NodeId, f32> = HashMap::new();
        // 记录前驱以回溯路径
        let mut came_from: HashMap<NodeId, NodeId> = HashMap::new();
        // 最小堆：按 f = g + h 排序
        let mut open: BinaryHeap<std::cmp::Reverse<OpenNode>> = BinaryHeap::new();

        g_score.insert(start, 0.0);
        let h0 = heuristic(map, start, goal_loc);
        open.push(std::cmp::Reverse(OpenNode {
            estimate: Cost(h0),
            node: start,
        }));

        while let Some(std::cmp::Reverse(OpenNode { node: current, .. })) = open.pop() {
            if current == goal {
                return Some(reconstruct(&came_from, current, start));
            }
            let current_g = *g_score.get(&current)?;
            let edges = map.edges_from(current)?;
            for edge in edges.iter() {
                let tentative_g = current_g + edge.cost;
                let better = tentative_g < *g_score.get(&edge.to).unwrap_or(&f32::INFINITY);
                if better {
                    came_from.insert(edge.to, current);
                    g_score.insert(edge.to, tentative_g);
                    let f = tentative_g + heuristic(map, edge.to, goal_loc);
                    open.push(std::cmp::Reverse(OpenNode {
                        estimate: Cost(f),
                        node: edge.to,
                    }));
                }
            }
        }
        None // 不可达
    }
}

/// 启发函数：当前节点到终点的欧氏距离；缺坐标时退化为 0（退化为 Dijkstra）。
#[inline]
fn heuristic(map: &Map, from: NodeId, goal_loc: &crate::domain::Location) -> f32 {
    map.node_location(from)
        .map(|l| l.distance_to(goal_loc))
        .unwrap_or(0.0)
}

/// 由前驱表回溯出从 start 到 goal 的路径。
fn reconstruct(came_from: &HashMap<NodeId, NodeId>, mut current: NodeId, start: NodeId) -> Path {
    let mut reversed: Vec<NodeId> = Vec::new();
    loop {
        reversed.push(current);
        if current == start {
            break;
        }
        match came_from.get(&current) {
            Some(&prev) => current = prev,
            None => break,
        }
    }
    let mut path = Path::new();
    for n in reversed.into_iter().rev() {
        path.push(n);
    }
    path
}

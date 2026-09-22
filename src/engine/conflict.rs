//! 多车路径冲突与死锁检测 (Conflict & Deadlock Detection)
//!
//! 这是一个**只读的分析器**：给定一帧全部机器人状态快照，基于“当前占用节点”与
//! “规划目的地”推导三类风险，供上层可视化，不参与实际调度决策：
//!
//! 1. **节点拥挤 (crowded)**：同一节点上同时有多台机器人（物理冲突点）；
//! 2. **目标争用 (contested)**：多台忙碌机器人以同一节点为目的地（潜在抢道）；
//! 3. **死锁环 (deadlock_cycles)**：构造“等待-持有”有向图——忙碌机器人 b 的目标节点
//!    若被另一机器人 h 当前占据，则 b 等待 h（边 `b -> h`）。对该图做 DFS 找环，
//!    环内机器人彼此互等、无法推进，即判定为死锁。
//!
//! 算法复杂度与在线机器人数线性相关（每个节点至多一条等待边集合），每帧调用可接受。

use std::collections::{HashMap, HashSet};

use crate::domain::{NodeId, RobotId, RobotState, RobotStatus};

/// 一次分析的冲突报告。
#[derive(Debug, Clone, Default)]
pub struct ConflictReport {
    /// 拥挤节点：`节点 -> 其上机器人列表`（长度 > 1）
    pub crowded: Vec<(NodeId, Vec<RobotId>)>,
    /// 争用目标：`目的地节点 -> 以它为目标的忙碌机器人列表`（长度 > 1）
    pub contested: Vec<(NodeId, Vec<RobotId>)>,
    /// 等待边：`(等待者, 持有者)`
    pub wait_edges: Vec<(RobotId, RobotId)>,
    /// 死锁环：每个环是一串互相等待的机器人 id（按等待方向排列）
    pub deadlock_cycles: Vec<Vec<RobotId>>,
}

impl ConflictReport {
    /// 是否检出任何风险。
    #[inline]
    pub fn has_issue(&self) -> bool {
        !self.crowded.is_empty() || !self.contested.is_empty() || !self.deadlock_cycles.is_empty()
    }

    /// 汇总所有处于死锁环中的机器人。
    pub fn deadlock_robots(&self) -> HashSet<RobotId> {
        self.deadlock_cycles.iter().flatten().copied().collect()
    }
}

/// 单帧死锁环输出上限（防御性，避免异常数据导致渲染爆量）。
const MAX_CYCLES: usize = 64;

/// 对给定机器人快照执行冲突/死锁分析。
pub fn analyze(robots: &[RobotState]) -> ConflictReport {
    let mut report = ConflictReport::default();

    // 1) 当前节点占用表（忽略离线车）
    let mut occupancy: HashMap<NodeId, Vec<RobotId>> = HashMap::new();
    for r in robots {
        if r.status == RobotStatus::Offline {
            continue;
        }
        if let Some(n) = r.location.node {
            occupancy.entry(n).or_default().push(r.id);
        }
    }
    report.crowded = occupancy
        .iter()
        .filter(|(_, v)| v.len() > 1)
        .map(|(k, v)| (*k, sorted_robots(v)))
        .collect();

    // 2) 忙碌机器人的目的地争用
    let mut goals: HashMap<NodeId, Vec<RobotId>> = HashMap::new();
    for r in robots {
        if r.status == RobotStatus::Busy
            && let Some(t) = r.target
        {
            goals.entry(t).or_default().push(r.id);
        }
    }
    report.contested = goals
        .iter()
        .filter(|(_, v)| v.len() > 1)
        .map(|(k, v)| (*k, sorted_robots(v)))
        .collect();

    // 3) 等待-持有图：忙碌车 b 的目标节点被其它车 h 占据 => 边 b -> h
    let mut adj: HashMap<RobotId, Vec<RobotId>> = HashMap::new();
    for r in robots {
        if r.status != RobotStatus::Busy {
            continue;
        }
        let Some(t) = r.target else { continue };
        if let Some(holders) = occupancy.get(&t) {
            for &h in holders {
                if h != r.id {
                    report.wait_edges.push((r.id, h));
                    adj.entry(r.id).or_default().push(h);
                }
            }
        }
    }

    report.deadlock_cycles = find_cycles(&adj);
    report
}

/// 在对有向图做 DFS 寻找环。返回去重后的若干环（每环 ≥ 2 个节点）。
fn find_cycles(adj: &HashMap<RobotId, Vec<RobotId>>) -> Vec<Vec<RobotId>> {
    // color: 0=未访问, 1=在当前路径(灰), 2=已完成(黑)
    let mut color: HashMap<RobotId, u8> = HashMap::new();
    let mut path: Vec<RobotId> = Vec::new();
    let mut result: Vec<Vec<RobotId>> = Vec::new();
    let mut seen: Vec<Vec<RobotId>> = Vec::new();

    let roots: Vec<RobotId> = adj.keys().copied().collect();
    for root in roots {
        if color.get(&root) == Some(&2) {
            continue;
        }
        dfs(root, adj, &mut color, &mut path, &mut result, &mut seen);
        if result.len() >= MAX_CYCLES {
            break;
        }
    }
    result
}

fn dfs(
    u: RobotId,
    adj: &HashMap<RobotId, Vec<RobotId>>,
    color: &mut HashMap<RobotId, u8>,
    path: &mut Vec<RobotId>,
    result: &mut Vec<Vec<RobotId>>,
    seen: &mut Vec<Vec<RobotId>>,
) {
    color.insert(u, 1);
    path.push(u);

    if let Some(nbrs) = adj.get(&u) {
        for &v in nbrs {
            match color.get(&v).copied().unwrap_or(0) {
                0 => {
                    if result.len() >= MAX_CYCLES {
                        break;
                    }
                    dfs(v, adj, color, path, result, seen);
                }
                1 => {
                    // 回边指向仍在路径中的 v：path[idx..] 即为一个环
                    if let Some(idx) = path.iter().position(|&x| x == v) {
                        let cycle: Vec<RobotId> = path[idx..].to_vec();
                        let mut key = cycle.clone();
                        key.sort_by_key(|r| r.0);
                        key.dedup();
                        if !seen.contains(&key) {
                            seen.push(key);
                            result.push(cycle);
                        }
                    }
                }
                _ => {} // 2：已完成，跳过
            }
        }
    }

    path.pop();
    color.insert(u, 2);
}

/// 对机器人列表按 id 排序（渲染稳定顺序）。
fn sorted_robots(v: &[RobotId]) -> Vec<RobotId> {
    let mut out = v.to_vec();
    out.sort_by_key(|r| r.0);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Battery, Location, TaskId};

    fn at(n: u32) -> Location {
        Location::at_node(NodeId(n), n as f32, 0.0)
    }

    fn busy(id: u64, at_node: u32, target: u32) -> RobotState {
        RobotState {
            id: RobotId(id),
            status: RobotStatus::Busy,
            location: at(at_node),
            battery: Battery::new(80),
            last_heartbeat: 0,
            current_task: Some(TaskId(id * 10)),
            target: Some(NodeId(target)),
            route: Vec::new(),
            last_change_ms: 0,
            carrying: false,
        }
    }

    #[test]
    fn detects_two_cycle_deadlock() {
        // A 占 N1 要 N2；B 占 N2 要 N1 -> 互相等待，构成一个环。
        let robots = vec![busy(1, 1, 2), busy(2, 2, 1)];
        let rep = analyze(&robots);
        assert_eq!(rep.deadlock_cycles.len(), 1, "应检出恰好一个死锁环");
        let cycle = &rep.deadlock_cycles[0];
        assert_eq!(cycle.len(), 2);
        assert!(rep.deadlock_robots().len() == 2);
        assert!(rep.has_issue());
    }

    #[test]
    fn no_cycle_for_one_way_wait() {
        // A->B 单向等待（B 的目标无人占），不构成环。
        let robots = vec![busy(1, 1, 2), busy(2, 2, 3)];
        let rep = analyze(&robots);
        assert!(rep.deadlock_cycles.is_empty(), "单向等待不应被判为死锁");
        assert_eq!(rep.wait_edges.len(), 1);
    }

    #[test]
    fn detects_crowded_node() {
        // 两台机器人同占 N1 -> 拥挤。
        let mut a = busy(1, 1, 2);
        let mut b = busy(2, 1, 3);
        a.target = None;
        b.target = None;
        a.status = RobotStatus::Idle;
        b.status = RobotStatus::Idle;
        let rep = analyze(&[a, b]);
        assert_eq!(rep.crowded.len(), 1);
        assert_eq!(rep.crowded[0].1.len(), 2);
    }

    #[test]
    fn detects_three_cycle() {
        // A占N1要N2，B占N2要N3，C占N3要N1 -> 三元环。
        let robots = vec![busy(1, 1, 2), busy(2, 2, 3), busy(3, 3, 1)];
        let rep = analyze(&robots);
        assert_eq!(rep.deadlock_cycles.len(), 1);
        assert_eq!(rep.deadlock_cycles[0].len(), 3);
    }
}

//! 机器人领域模型 (Robot Domain Model)
//!
//! 描述单台 AGV/AMR 的实时状态：位置、电量、运行状态机、心跳与当前绑定任务。
//! [`RobotState`] 是系统在高频上报路径上被反复读写的核心结构，
//! 因此将其设计为可整体 `Clone` 的“值快照”，配合无锁的 `DashMap` 存储可实现安全的并发替换。

use serde::{Deserialize, Serialize};

use super::map::{Location, NodeId};
use super::task::TaskId;

/// 机器人唯一标识。`u64` newtype，`Copy` + `Hash`，可直接作为并发容器 key。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RobotId(pub u64);

impl std::fmt::Display for RobotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ROBOT-{:04}", self.0)
    }
}

/// 单调递增的时间戳（毫秒）。系统内部统一使用“开机后的毫秒数”，避免时钟回拨问题。
pub type TimestampMs = u64;

/// 电量。使用 `u8` 百分比即可满足调度决策，`Copy` 零开销。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Battery(pub u8);

impl Battery {
    /// 低于该阈值时需要优先安排回充（且不再分配普通搬运任务）。
    pub const LOW_THRESHOLD: u8 = 20;
    /// 回充到该阈值即视为充满，恢复空闲可分配状态。
    pub const FULL_THRESHOLD: u8 = 95;

    pub fn new(percent: u8) -> Self {
        Self(percent.min(100))
    }

    #[inline]
    pub fn is_low(&self) -> bool {
        self.0 < Self::LOW_THRESHOLD
    }

    /// 是否已充到可恢复工作的高电量。
    #[inline]
    pub fn is_full(&self) -> bool {
        self.0 >= Self::FULL_THRESHOLD
    }
}

/// 机器人运行状态机。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RobotStatus {
    /// 已上线但空闲，可接任务
    Idle,
    /// 正在执行任务
    Busy,
    /// 回充中
    Charging,
    /// 故障/急停
    Error,
    /// 心跳超时，判定离线
    Offline,
}

impl RobotStatus {
    /// 是否可被调度器分配新任务。
    #[inline]
    pub fn is_available(&self) -> bool {
        matches!(self, RobotStatus::Idle)
    }
}

/// 机器人完整状态快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RobotState {
    pub id: RobotId,
    pub status: RobotStatus,
    pub location: Location,
    pub battery: Battery,
    /// 最近一次心跳时间戳（毫秒）
    pub last_heartbeat: TimestampMs,
    /// 当前绑定并执行的任务（若有）
    pub current_task: Option<TaskId>,
    /// 当前规划目的地节点（执行任务时的目标节点，空闲/完成时为 None）。
    /// 供冲突/死锁检测与轨迹可视化使用。
    pub target: Option<NodeId>,
    /// 当前规划的完整行驶路线（节点序列，含起点与终点；空闲时为空）。
    /// 供 GUI 绘制“前方计划路线 + 目的地”，以及行驶中沿边插值预测使用。
    #[serde(default)]
    pub route: Vec<NodeId>,
    /// 最近一次所在节点发生变化的时刻（毫秒）；供 GUI 计算逐段行驶时长做平滑插值。
    #[serde(default)]
    pub last_change_ms: TimestampMs,
    /// 当前是否载货（取货送达任务驶向放货点的途中为 true）；空闲/完成/回充时为 false。
    /// 由心跳上报驱动，供 GUI 在机器人上叠加货箱图标。
    #[serde(default)]
    pub carrying: bool,
}

impl RobotState {
    /// 以上线空闲状态构造一台新注册的机器人。
    pub fn onboard(id: RobotId, location: Location, battery: Battery, now: TimestampMs) -> Self {
        Self {
            id,
            status: RobotStatus::Idle,
            location,
            battery,
            last_heartbeat: now,
            current_task: None,
            target: None,
            route: Vec::new(),
            last_change_ms: now,
            carrying: false,
        }
    }

    /// 判断在给定时钟下心跳是否已超时。
    #[inline]
    pub fn is_stale(&self, now: TimestampMs, timeout_ms: u64) -> bool {
        now.saturating_sub(self.last_heartbeat) > timeout_ms
    }
}

/// 机器人上报的心跳包（轻量，走二进制通道）。
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Heartbeat {
    pub id: RobotId,
    pub battery: Battery,
    pub location: Location,
    pub status: RobotStatus,
    pub timestamp: TimestampMs,
    /// 是否载货（取货送达任务取货后、送达前为 true）。随心跳上报刷新 `RobotState::carrying`。
    #[serde(default)]
    pub carrying: bool,
}

//! 系统配置与共享基础设施 (Configuration & Shared Infrastructure)
//!
//! [`SystemConfig`] 集中管理调度系统的可调参数（超时、心跳、tick 周期、监听地址等）。
//! 同时提供一个进程级单调时钟工具 [`monotonic_ms`]，全系统统一以“进程启动后的毫秒数”为时间基准，
//! 规避 wall-clock 回拨 / NTP 跳变带来的状态误判。

use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// 调度系统运行期配置。
#[derive(Debug, Clone)]
pub struct SystemConfig {
    /// TCP 服务端监听地址
    pub listen_addr: String,
    /// 心跳超时（毫秒）：超过该时长未收到心跳即判定离线
    pub heartbeat_timeout_ms: u64,
    /// 调度器主循环 tick 间隔（毫秒）——决定任务分配延迟的量级
    pub scheduler_tick_ms: u64,
    /// 事件通道容量（有界，提供背压）
    pub event_channel_capacity: usize,
    /// 任务分配后无进展的回收阈值（毫秒）：超时则回收重试或判定失败
    pub task_timeout_ms: u64,
    /// 单个任务被回收重试的次数上限；超过则置为失败 (Failed)
    pub max_task_retries: u8,
    /// 优先级老化步长（毫秒）：任务每等待该时长，有效优先级提升一级（封顶 Urgent），防饥饿
    pub priority_aging_ms: u64,
    /// 死锁解除中被“假释让路”机器人的冷却窗口（毫秒）：窗口内不参与分配，避免立即重排回同一环
    pub deadlock_cooldown_ms: u64,
    /// 是否启用死锁自动解除（关闭时仅检测可视化，不干预调度）
    pub enable_deadlock_resolution: bool,
}

impl Default for SystemConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:0".to_string(),
            heartbeat_timeout_ms: 5_000,
            scheduler_tick_ms: 5, // 毫秒级调度延迟
            event_channel_capacity: 65_536,
            task_timeout_ms: 8_000,
            max_task_retries: 3,
            priority_aging_ms: 4_000,
            deadlock_cooldown_ms: 1_500,
            enable_deadlock_resolution: true,
        }
    }
}

/// 进程启动锚点，用于计算单调毫秒时间戳。
static START: OnceLock<Instant> = OnceLock::new();

/// 返回进程启动至今的单调毫秒数。线程安全、无锁（`OnceLock` 初始化后为只读）。
#[inline]
pub fn monotonic_ms() -> u64 {
    let start = START.get_or_init(Instant::now);
    start.elapsed().as_millis() as u64
}

/// 便捷构造 `Duration::from_millis`。
#[inline]
pub fn millis(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

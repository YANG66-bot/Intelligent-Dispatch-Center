//! 运行指标采集 (Metrics & Throughput History)
//!
//! 以固定周期对 [`MemoryStore`] 的累计计数器做一次快照，构成一条可回放的
//! “吞吐历史”：心跳、提交、分配、完成的累计值，以及在线/待处理的瞬时值。
//!
//! # 设计
//! - [`MetricsHistory`] 内部以 `Arc<parking_lot::Mutex<VecDeque<Sample>>>` 承载环形缓冲，
//!   `Clone` 廉价且共享——后台采集任务写入、GUI 每帧只读快照，两端指向同一份历史；
//! - 采集与渲染解耦：这里只记录**累计量**，速率（次/秒）与完成率等派生指标由上层
//!   在相邻采样点间求差计算，避免采集线程持有浮点状态、也便于改变窗口。

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::config::monotonic_ms;
use crate::storage::MemoryStore;

/// 单个采样点：记录采样时刻与各累计计数器的**当前总量**。
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// 采样时间（单调毫秒）
    pub t_ms: u64,
    /// 累计处理心跳数
    pub heartbeats: u64,
    /// 累计提交任务数
    pub submitted: u64,
    /// 累计分配次数
    pub assigned: u64,
    /// 累计完成次数
    pub completed: u64,
    /// 采样时刻在线机器人数
    pub online: u64,
    /// 采样时刻待处理任务数
    pub pending: u64,
    /// 累计自动解除的死锁环数
    pub deadlock_resolved: u64,
    /// 累计被回收重试的任务数
    pub task_retried: u64,
}

/// 可共享的吞吐历史环形缓冲。
#[derive(Clone, Default)]
pub struct MetricsHistory {
    inner: Arc<Mutex<VecDeque<Sample>>>,
}

/// 保留的最大采样点数。默认 500ms 采样下约覆盖 2 分钟窗口。
const HISTORY_CAPACITY: usize = 240;

impl MetricsHistory {
    /// 追加一个采样点，超出容量时丢弃最旧。
    pub fn push(&self, s: Sample) {
        let mut q = self.inner.lock();
        q.push_back(s);
        while q.len() > HISTORY_CAPACITY {
            q.pop_front();
        }
    }

    /// 导出当前历史快照（oldest → newest），供 GUI 绘制折线。
    pub fn snapshot(&self) -> Vec<Sample> {
        self.inner.lock().iter().copied().collect()
    }
}

/// 后台采集循环：每隔 `interval_ms` 对 store 做一次快照写入 history。
/// 应通过 `tokio::spawn(run_collector(..))` 启动。
pub async fn run_collector(store: MemoryStore, history: MetricsHistory, interval_ms: u64) {
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let sample = Sample {
            t_ms: monotonic_ms(),
            heartbeats: store.heartbeat_count(),
            submitted: store.task_submit_count(),
            assigned: store.assigned_count(),
            completed: store.completed_count(),
            online: store.online_robot_count() as u64,
            pending: store.pending_count() as u64,
            deadlock_resolved: store.deadlock_resolved_count(),
            task_retried: store.task_retried_count(),
        };
        history.push(sample);
    }
}

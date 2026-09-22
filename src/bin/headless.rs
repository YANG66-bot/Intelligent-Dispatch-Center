//! 无头 CLI 演示 (Headless Demo)
//!
//! 不用 GUI，纯日志地演示调度闭环：装配服务 -> 接入 5 台虚拟机器人 -> 持续提交任务 ->
//! 运行若干秒后打印统计并退出。运行：`cargo run --bin headless`。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;

use app::config::SystemConfig;
use app::domain::TaskStatus;
use app::service::Service;
use app::simulator::{spawn_robot_fleet, spawn_task_stream};

#[tokio::main(worker_threads = 4)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let service = Service::start(SystemConfig::default())
        .await
        .expect("启动调度服务失败");
    tracing::info!("服务已启动，监听 {}", service.addr);

    let rt = tokio::runtime::Handle::current();

    // 接入 5 台机器人
    let _fleet = spawn_robot_fleet(&rt, service.map.clone(), service.addr, 5);

    // 提交 15 个随机任务后自动停止任务流
    let running = Arc::new(AtomicBool::new(true));
    let seq = Arc::new(AtomicU64::new(1));
    let producer = spawn_task_stream(
        &rt,
        service.handle.clone(),
        service.map.clone(),
        400,
        running.clone(),
        seq.clone(),
    );

    // 让系统跑一会儿
    tokio::time::sleep(Duration::from_secs(6)).await;
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    producer.abort();

    // 打印统计
    let store = &service.store;
    let completed = store
        .all_tasks()
        .into_iter()
        .filter(|t| matches!(t.status, TaskStatus::Completed))
        .count();
    tracing::info!("──────────── 运行统计 ────────────");
    tracing::info!("注册机器人总数 : {}", store.robot_count());
    tracing::info!("当前活动连接数 : {}", service.server.connection_count());
    tracing::info!("累计处理心跳数 : {}", store.heartbeat_count());
    tracing::info!("累计提交任务数 : {}", store.task_submit_count());
    tracing::info!("已完成任务数   : {}", completed);

    service.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;
}

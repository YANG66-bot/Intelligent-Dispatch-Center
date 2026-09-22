//! 高性能机器人调度系统 —— 库根 (Library Root)
//!
//! 把核心分层（domain / engine / storage / transport / config）以 `pub` 形式对外暴露，
//! 使同一套内核既可被 **无头 CLI 演示**（`src/bin/headless.rs`）复用，
//! 也可被 **GUI 监控面板**（`src/main.rs`）复用。
//!
//! 另提供两个面向组装的模块：
//! - [`service`]：一键装配并启动后台调度器 + TCP 服务端，产出可被 UI 读取的共享状态；
//! - [`simulator`]：模拟机器人 TCP 客户端与任务流发生器，供演示/压测复用。

// 核心骨架包含大量“面向扩展”的 public API，在演示程序里未必全部被调用，统一放行。
#![allow(dead_code)]

pub mod config;
pub mod domain;
pub mod engine;
pub mod metrics;
pub mod service;
pub mod simulator;
pub mod storage;
pub mod transport;

/// 版本号，供 UI/健康检查展示。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

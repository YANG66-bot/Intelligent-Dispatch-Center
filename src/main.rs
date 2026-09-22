//! AGV/AMR 智能调度中心 —— GUI 监控面板 (Slint Desktop Dashboard)
//!
//! # 运行模型
//! 主线程运行 Slint 事件循环，后台 tokio 多线程运行时驱动调度器 / 服务端 / 机器人客户端。
//! 二者通过 `Arc` 共享的 [`MemoryStore`] 快照衔接：Slint 侧用一个重复 `Timer` 每 ~100ms
//! 只读地把后端状态刷入界面属性，不持锁、不阻塞后端；用户操作则通过回调 +
//! `tokio::runtime::Handle::spawn` 反馈到后台。
//!
//! # 面板构成
//! - 顶部：系统指标卡片（在线/空闲/忙碌/待处理/执行中/已完成/心跳/连接）与仿真控制；
//! - 中央：实时路网拓扑图（节点、边、按状态着色的机器人、电量、轨迹与冲突叠加）；
//! - 右侧：机器人明细列表 与 任务看板；底部：吞吐历史折线图。
//!
//! 界面定义见 `ui/scheduler.slint`，由 `build.rs` 编译后经 `include_modules!` 引入。
//! 运行：`cargo run`（默认 bin 即本 GUI）。

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use app::config::SystemConfig;
use app::domain::{Map, NodeId, RobotId, RobotState, RobotStatus, Task, TaskStatus};
use app::engine::conflict;
use app::metrics::Sample;
use app::service::{Service, charger_nodes};
use app::simulator::{make_random_task, spawn_robot_fleet, spawn_task_stream};

use slint::{ComponentHandle, Model, ModelRc, SharedString, Timer, TimerMode, VecModel};

// 由 build.rs 编译 ui/scheduler.slint 生成的组件与结构体。
slint::include_modules!();

// ============================ 画布 / 图表尺寸常量 ============================
// 必须与 scheduler.slint 中 Canvas.w/h 及 ChartBox 绘图区(344x112)保持一致。
const MAP_W: f32 = 760.0;
const MAP_H: f32 = 470.0;
const MAP_PAD: f32 = 26.0;
const CHART_W: f32 = 344.0;
const CHART_H: f32 = 112.0;

/// 机器人悬浮最大抬升像素（电量 100% 时）；充电桩用固定较高悬浮。
const MAX_LIFT: f32 = 30.0;

/// 逐节点行驶时长（毫秒），须与 `simulator::MS_PER_NODE` 保持一致；GUI 据此预测插值动画。
const NODE_MS: f32 = 160.0;

fn main() -> Result<(), slint::PlatformError> {
    init_tracing();

    // 后台 tokio 运行时（多线程）：调度/网络/仿真都跑在这里，须存活至进程结束。
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .expect("构建 tokio 运行时失败");

    // 在运行时上下文中装配后端服务；block_on 返回后工作线程仍持续驱动已 spawn 的任务。
    let service = rt
        .block_on(Service::start(SystemConfig::default()))
        .expect("启动调度服务失败");
    let handle = rt.handle().clone();
    tracing::info!("调度后端已启动，GUI 监听地址 {}", service.addr);

    // 创建主窗口。Slint 的 ComponentHandle 为 Rc<组件>，故包裹进 Rc 以便 as_weak/run_event_loop。
    let ui = Rc::new(MainWindow::new()?);
    let state = Rc::new(RefCell::new(AppState::new(rt, handle, service)));

    // 静态路网（节点/充电桩/边/等距地板）由首帧 refresh 依投影模式重建下发。
    // 机器人常驻模型一次性挂接，之后每帧仅就地更新行数据（不替换 ModelRc）。
    ui.set_robots(ModelRc::from(state.borrow().robots_model.clone()));

    // 接线交互回调。回调内只改动共享状态（RefCell），实际渲染由 Timer 周期刷新完成。
    {
        let s = Rc::clone(&state);
        ui.on_connect_fleet(move |n| s.borrow_mut().relaunch_fleet(n as i64));

        let s = Rc::clone(&state);
        ui.on_disconnect_fleet(move || s.borrow_mut().disconnect_all());

        let s = Rc::clone(&state);
        ui.on_submit_one(move || s.borrow_mut().submit_one());

        let s = Rc::clone(&state);
        ui.on_select_robot(move |id| {
            let mut st = s.borrow_mut();
            st.selected = Some(RobotId(id as u64));
        });

        let s = Rc::clone(&state);
        ui.on_clear_selection(move || s.borrow_mut().selected = None);
    }

    // 周期刷新：~30fps（33ms）采集一帧只读快照并沿计划路线插值，驱动平滑行驶动画。
    let timer = Timer::default();
    {
        let s2 = Rc::clone(&state);
        let weak = ui.as_weak();
        timer.start(TimerMode::Repeated, Duration::from_millis(33), move || {
            let Some(ui) = weak.upgrade() else { return };
            let mut st = s2.borrow_mut();
            st.refresh(&ui);
        });
    }

    // 阻塞式事件循环（Slint 必须运行在主线程）。
    let result = ui.run();

    // 退出时优雅停机。
    state.borrow_mut().cleanup();
    result
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
}

/// 世界坐标 → 画布地面像素的投影变换（等距/顶视 + 包围盒自适应居中）。
#[derive(Clone, Copy)]
struct Geo {
    iso: bool,
    min_u: f32,
    min_v: f32,
    scale: f32,
    off_x: f32,
    off_y: f32,
}

/// 2:1 等距投影（dimetric）：世界 (x,y) → 投影 (u,v)，未缩放未居中。
#[inline]
fn iso_project(x: f32, y: f32) -> (f32, f32) {
    (x - y, (x + y) * 0.5)
}

/// 顶视退化投影：保持世界坐标。
#[inline]
fn top_project(x: f32, y: f32) -> (f32, f32) {
    (x, y)
}

/// 悬浮高度：随电量线性映射到 `[0, MAX_LIFT]`（低电量贴地、满电悬浮最高）。
#[inline]
fn lift_for(battery: u8) -> f32 {
    (battery.min(100) as f32 / 100.0) * MAX_LIFT
}

/// 依投影模式对全部世界坐标点投影，求屏幕包围盒并等比缩放 + 居中（含顶部悬浮留白）。
fn compute_geo(iso: bool, pts: &[(f32, f32)]) -> Geo {
    let (mut min_u, mut min_v) = (f32::INFINITY, f32::INFINITY);
    let (mut max_u, mut max_v) = (f32::NEG_INFINITY, f32::NEG_INFINITY);
    for &(x, y) in pts {
        let (u, v) = if iso { iso_project(x, y) } else { (x, y) };
        min_u = min_u.min(u);
        max_u = max_u.max(u);
        min_v = min_v.min(v);
        max_v = max_v.max(v);
    }
    if !min_u.is_finite() {
        min_u = 0.0;
        min_v = 0.0;
        max_u = 1.0;
        max_v = 1.0;
    }
    let span_u = (max_u - min_u).max(1.0);
    let span_v = (max_v - min_v).max(1.0);
    // 等距模式下为本体抬升预留顶部空间，避免悬浮机器人被裁切。
    let headroom = if iso { MAX_LIFT + 10.0 } else { 0.0 };
    let avail_w = MAP_W - 2.0 * MAP_PAD;
    let avail_h = MAP_H - 2.0 * MAP_PAD - headroom;
    let scale = (avail_w / span_u).min(avail_h / span_v);
    let off_x = MAP_PAD + (avail_w - span_u * scale) / 2.0;
    let off_y = MAP_PAD + headroom + (avail_h - span_v * scale) / 2.0;
    Geo {
        iso,
        min_u,
        min_v,
        scale,
        off_x,
        off_y,
    }
}

/// GUI 侧全部可变状态。持有 `Runtime` 以保证后台任务存活。
struct AppState {
    _rt: tokio::runtime::Runtime,
    handle: tokio::runtime::Handle,
    service: Service,

    // ---- 仿真控制 ----
    robot_handles: Vec<tokio::task::JoinHandle<()>>,
    producer_handle: Option<tokio::task::JoinHandle<()>>,
    auto_tasks: bool,
    task_running: Arc<AtomicBool>,
    task_seq: Arc<AtomicU64>,

    // ---- 可视化交互 ----
    selected: Option<RobotId>,
    /// 机器人常驻模型：逐帧用 `set_row_data` 就地更新，避免整体替换导致
    /// `for` 元素与 `TouchArea` 被销毁重建而丢失点击（按下/释放须在同一实例）。
    robots_model: Rc<VecModel<RobotInfo>>,

    // ---- 静态几何（按投影模式重建）----
    geo: Geo,
    node_ground: HashMap<NodeId, (f32, f32)>,
    statics_dirty: bool,
}

impl AppState {
    fn new(rt: tokio::runtime::Runtime, handle: tokio::runtime::Handle, service: Service) -> Self {
        // 初始按等距模式计算几何；静态模型由首帧 refresh 依 statics_dirty 重建下发。
        let pts: Vec<(f32, f32)> = service
            .map
            .all_node_locations()
            .into_iter()
            .map(|(_, l)| (l.x, l.y))
            .collect();
        let geo = compute_geo(true, &pts);

        Self {
            _rt: rt,
            handle,
            service,
            robot_handles: Vec::new(),
            producer_handle: None,
            auto_tasks: false,
            task_running: Arc::new(AtomicBool::new(false)),
            task_seq: Arc::new(AtomicU64::new(1)),
            selected: None,
            robots_model: Rc::new(VecModel::default()),
            geo,
            node_ground: HashMap::new(),
            statics_dirty: true,
        }
    }

    /// 依投影模式重建静态模型（节点/充电桩/边/等距地板）并下发到界面。
    fn rebuild_statics(&mut self, ui: &MainWindow, iso: bool) {
        let all = self.service.map.all_node_locations();
        let pts: Vec<(f32, f32)> = all.iter().map(|(_, l)| (l.x, l.y)).collect();
        let geo = compute_geo(iso, &pts);

        // 节点地面坐标 + 模型（按 v 升序，画家算法远处先画）。
        let mut node_ground = HashMap::new();
        let mut nodes: Vec<NodeInfo> = Vec::new();
        for (id, loc) in &all {
            let p = geo.to_ground(loc.x, loc.y);
            node_ground.insert(*id, p);
            nodes.push(NodeInfo { sx: p.0, sy: p.1 });
        }
        nodes.sort_by(|a, b| {
            a.sy.partial_cmp(&b.sy)
                .unwrap()
                .then(a.sx.partial_cmp(&b.sx).unwrap())
        });
        ui.set_nodes(ModelRc::from(Rc::new(VecModel::from(nodes))));

        // 充电桩（取路网角点）。
        let chargers: Vec<NodeInfo> = charger_nodes(&self.service.map)
            .iter()
            .filter_map(|n| node_ground.get(n))
            .map(|&(x, y)| NodeInfo { sx: x, sy: y })
            .collect();
        ui.set_chargers(ModelRc::from(Rc::new(VecModel::from(chargers))));

        // 边：贴地单一 SVG 路径字符串。
        let mut ep = String::new();
        for e in self.service.map.all_edges() {
            if let (Some(&a), Some(&b)) = (node_ground.get(&e.from), node_ground.get(&e.to)) {
                ep.push_str(&format!("M {:.1} {:.1} L {:.1} {:.1} ", a.0, a.1, b.0, b.1));
            }
        }
        ui.set_edges_path(s(ep));

        // 等距棋盘地板：浅/深两块 + 网格描边。
        let (light, dark, grid) = floor_paths(&geo, &self.service.map);
        ui.set_floor_light(s(light));
        ui.set_floor_dark(s(dark));
        ui.set_floor_grid(s(grid));

        self.geo = geo;
        self.node_ground = node_ground;
    }

    // ---- 仿真控制操作 ----

    /// 重启指定数量机器人集群：先断开旧的，再接入新的。
    fn relaunch_fleet(&mut self, fleet_size: i64) {
        for h in self.robot_handles.drain(..) {
            h.abort();
        }
        let n = fleet_size.max(1) as u64;
        self.robot_handles =
            spawn_robot_fleet(&self.handle, self.service.map.clone(), self.service.addr, n);
    }

    fn disconnect_all(&mut self) {
        for h in self.robot_handles.drain(..) {
            h.abort();
        }
    }

    fn set_auto_tasks(&mut self, on: bool) {
        self.auto_tasks = on;
        if on {
            self.task_running.store(true, Ordering::Relaxed);
            let h = spawn_task_stream(
                &self.handle,
                self.service.handle.clone(),
                self.service.map.clone(),
                450,
                self.task_running.clone(),
                self.task_seq.clone(),
            );
            self.producer_handle = Some(h);
        } else {
            self.task_running.store(false, Ordering::Relaxed);
            if let Some(h) = self.producer_handle.take() {
                h.abort();
            }
        }
    }

    fn submit_one(&mut self) {
        let task = make_random_task(&self.service.map, &self.task_seq);
        let h = self.service.handle.clone();
        self.handle.spawn(async move {
            let _ = h.submit_task(task);
        });
    }

    fn cleanup(&mut self) {
        self.set_auto_tasks(false);
        self.disconnect_all();
        self.service.shutdown();
    }

    // ---- 周期刷新 ----

    fn refresh(&mut self, ui: &MainWindow) {
        // 读取双向 UI 状态。
        let auto = ui.get_auto_tasks();
        if auto != self.auto_tasks {
            self.set_auto_tasks(auto);
        }
        let show_conflict = ui.get_show_conflict();

        // 投影模式变化或首帧时重建静态模型（节点/充电桩/边/地板）。
        let iso = ui.get_view_iso();
        if iso != self.geo.iso || self.statics_dirty {
            self.rebuild_statics(ui, iso);
            self.statics_dirty = false;
        }

        // 采集一帧只读快照。
        let robots = self.service.store.all_robots();
        let tasks = self.service.store.all_tasks();
        let report = conflict::analyze(&robots);
        let deadlocked: HashSet<RobotId> = report.deadlock_robots();
        let crowded_nodes: HashSet<RobotId> = if show_conflict {
            report
                .crowded
                .iter()
                .flat_map(|(_, v)| v.iter())
                .copied()
                .collect()
        } else {
            HashSet::new()
        };

        // 机器人模型：沿计划路线预测插值→地面投影→悬浮抬升（编码电量），按地面 y 升序做画家算法。
        let now_ms = app::config::monotonic_ms();
        let mut robot_infos: Vec<RobotInfo> = Vec::with_capacity(robots.len());
        let mut dests: Vec<DestInfo> = Vec::new();
        for r in robots.iter() {
            let (wx, wy) = self.anim_world(r, now_ms);
            let (gx0, gy0) = self.geo.to_ground(wx, wy);
            let (jx, jy) = jitter(r.id.0);
            let gx = gx0 + jx;
            let gy = gy0 + jy;
            let lift = if self.geo.iso {
                lift_for(r.battery.0)
            } else {
                0.0
            };
            let selected = self.selected == Some(r.id);
            // 目的地标记（有目标的机器人）：地面坐标 + 是否选中。
            if let Some(t) = r.target
                && let Some(&(dx, dy)) = self.node_ground.get(&t)
            {
                dests.push(DestInfo {
                    sx: dx,
                    sy: dy,
                    sel: selected,
                });
            }
            robot_infos.push(RobotInfo {
                id: r.id.0 as i32,
                sx: gx,
                sy: gy - lift,
                gx,
                gy,
                lift,
                status: status_code(r.status),
                battery: r.battery.0 as i32,
                deadlocked: show_conflict && deadlocked.contains(&r.id),
                crowded: crowded_nodes.contains(&r.id),
                selected,
                carrying: r.carrying,
                tag: s(format!("#{}", r.id.0)),
                row: s(robot_row(r)),
            });
        }
        // 按 id 稳定排序：与常驻模型配合使索引↔机器人恒定，点击命中不会被每帧洗牌干扰。
        robot_infos.sort_by_key(|i| i.id);
        self.sync_robots(robot_infos);
        ui.set_dests(ModelRc::from(Rc::new(VecModel::from(dests))));

        // 选中机器人的前方完整计划路线（地面折线）。
        let route_line = self
            .selected
            .and_then(|id| robots.iter().find(|r| r.id == id))
            .map(|r| self.route_path(&r.route))
            .unwrap_or_default();
        ui.set_route_path(s(route_line));

        // 任务模型（新任务在前）。
        let mut task_pairs: Vec<(u64, TaskInfo)> = tasks
            .iter()
            .map(|t| {
                (
                    t.id.0,
                    TaskInfo {
                        row: s(task_row(t)),
                        status: task_status_code(t.status),
                    },
                )
            })
            .collect();
        task_pairs.sort_by_key(|p| std::cmp::Reverse(p.0));
        let task_infos: Vec<TaskInfo> =
            task_pairs.into_iter().take(200).map(|(_, ti)| ti).collect();
        ui.set_tasks(ModelRc::from(Rc::new(VecModel::from(task_infos))));

        // 冲突标记。
        let mut markers: Vec<MarkerInfo> = Vec::new();
        if show_conflict {
            for (node, list) in &report.crowded {
                if let Some(&(x, y)) = self.node_ground.get(node) {
                    markers.push(MarkerInfo {
                        sx: x,
                        sy: y,
                        kind: 0,
                        label: s(format!("×{}", list.len())),
                    });
                }
            }
            for (node, list) in &report.contested {
                if let Some(&(x, y)) = self.node_ground.get(node) {
                    markers.push(MarkerInfo {
                        sx: x,
                        sy: y,
                        kind: 1,
                        label: s(format!("?{}", list.len())),
                    });
                }
            }
        }
        ui.set_markers(ModelRc::from(Rc::new(VecModel::from(markers))));

        // 指标条。
        let online = robots
            .iter()
            .filter(|r| r.status != RobotStatus::Offline)
            .count();
        let idle = robots
            .iter()
            .filter(|r| r.status == RobotStatus::Idle)
            .count();
        let busy = robots
            .iter()
            .filter(|r| r.status == RobotStatus::Busy)
            .count();
        let pending = tasks
            .iter()
            .filter(|t| matches!(t.status, TaskStatus::Pending))
            .count();
        let running = tasks
            .iter()
            .filter(|t| matches!(t.status, TaskStatus::Assigned | TaskStatus::Executing))
            .count();
        let done = tasks
            .iter()
            .filter(|t| matches!(t.status, TaskStatus::Completed))
            .count();
        let hb = self.service.store.heartbeat_count() as usize;
        let conns = self.service.server.connection_count();
        ui.set_m_online(s(online.to_string()));
        ui.set_m_idle(s(idle.to_string()));
        ui.set_m_busy(s(busy.to_string()));
        ui.set_m_pending(s(pending.to_string()));
        ui.set_m_running(s(running.to_string()));
        ui.set_m_done(s(done.to_string()));
        ui.set_m_hb(s(hb.to_string()));
        ui.set_m_conn(s(conns.to_string()));
        ui.set_robots_title(s(format!("机器人明细 ({})", robots.len())));
        ui.set_tasks_title(s(format!("任务看板 ({})", tasks.len())));
        ui.set_map_hint(s(format!("在线 {online} 台 · 点击机器人查看轨迹与详情")));

        // 冲突态势条 + 调度事件（累计自动解除死锁 / 回收重试 / 失败）。
        let dl = self.service.store.deadlock_resolved_count();
        let rt = self.service.store.task_retried_count();
        let fl = self.service.store.task_failed_count();
        let mut evt: Vec<String> = Vec::new();
        if dl > 0 {
            evt.push(format!("解除死锁 {dl}"));
        }
        if rt > 0 {
            evt.push(format!("回收重试 {rt}"));
        }
        if fl > 0 {
            evt.push(format!("任务失败 {fl}"));
        }
        let evt_line = evt.join(" · ");

        if show_conflict && report.has_issue() {
            let n_dead = report
                .deadlock_cycles
                .iter()
                .map(|c| c.len())
                .sum::<usize>();
            let base = format!(
                "⚠ 风险：拥挤节点 {} · 目标争用 {} · 死锁机器人 {}",
                report.crowded.len(),
                report.contested.len(),
                n_dead
            );
            let line = if evt_line.is_empty() {
                base
            } else {
                format!("{base}  |  累计 {evt_line}")
            };
            ui.set_conflict_line(s(line));
        } else if !evt_line.is_empty() {
            ui.set_conflict_line(s(format!("调度事件：累计 {evt_line}")));
        } else {
            ui.set_conflict_line(SharedString::default());
        }

        // 选中机器人：轨迹折线 + 详情浮层。
        let sel_robot = self
            .selected
            .and_then(|id| robots.iter().find(|r| r.id == id).cloned());
        match sel_robot {
            Some(r) => {
                ui.set_sel_visible(true);
                let trail = self.service.store.robot_trail(r.id);
                ui.set_trail_path(s(self.trail_path(&trail)));
                ui.set_sel_title(s(format!("{}", r.id)));
                ui.set_sel_status(s(format!("状态：{}", status_str(r.status))));
                ui.set_sel_battery(s(format!("电量：{}%", r.battery.0)));
                ui.set_sel_node(s(format!(
                    "所在节点：{}",
                    r.location
                        .node
                        .map(|n| format!("N{}", n.0))
                        .unwrap_or_else(|| "-".into())
                )));
                ui.set_sel_target(s(format!(
                    "目的地：{}",
                    r.target
                        .map(|n| format!("N{}", n.0))
                        .unwrap_or_else(|| "-".into())
                )));
                ui.set_sel_trail(s(format!("轨迹点：{} 个采样", trail.len())));
                ui.set_sel_task(s(
                    match r
                        .current_task
                        .and_then(|tid| tasks.iter().find(|t| t.id == tid))
                    {
                        Some(t) => format!(
                            "当前任务 #{} · {} · 优先级 {} · {}{}",
                            t.id.0,
                            t.kind_desc(),
                            prio_str(t.priority),
                            if r.carrying { "载货中" } else { "空载" },
                            t.goal_node()
                                .map(|n| format!(" · 取 N{}", n.0))
                                .unwrap_or_default()
                        ),
                        None => "当前无绑定任务（空闲）".to_string(),
                    },
                ));
            }
            _ => {
                ui.set_sel_visible(false);
                ui.set_trail_path(SharedString::default());
            }
        }

        // 吞吐折线图。
        let samples = self.service.metrics.snapshot();
        if samples.len() >= 2 {
            let (hb_s, asg_s, rate_s) = build_series(&samples);
            ui.set_hb_path(s(series_path(&hb_s)));
            ui.set_asg_path(s(series_path(&asg_s)));
            ui.set_rate_path(s(series_path(&rate_s)));
            ui.set_hb_latest(s(last_label(&hb_s)));
            ui.set_asg_latest(s(last_label(&asg_s)));
            ui.set_rate_latest(s(last_label(&rate_s)));
        } else {
            ui.set_hb_path(SharedString::default());
            ui.set_asg_path(SharedString::default());
            ui.set_rate_path(SharedString::default());
            ui.set_hb_latest(s("采样中…"));
            ui.set_asg_latest(s("-"));
            ui.set_rate_latest(s("-"));
        }
    }

    /// 选中机器人历史轨迹的 SVG 路径字符串（地面坐标，z=0 贴地）。
    fn trail_path(&self, trail: &[(u64, app::domain::Location)]) -> String {
        if trail.len() < 2 {
            return String::new();
        }
        let mut p = String::new();
        for (i, (_, loc)) in trail.iter().enumerate() {
            let (x, y) = self.geo.to_ground(loc.x, loc.y);
            p.push_str(&if i == 0 {
                format!("M {x:.1} {y:.1}")
            } else {
                format!(" L {x:.1} {y:.1}")
            });
        }
        p
    }

    /// 选中机器人的完整计划路线 SVG 折线（地面坐标）。
    fn route_path(&self, route: &[NodeId]) -> String {
        let mut p = String::new();
        let mut first = true;
        for &n in route {
            if let Some(&(x, y)) = self.node_ground.get(&n) {
                p.push_str(&if first {
                    format!("M {x:.1} {y:.1}")
                } else {
                    format!(" L {x:.1} {y:.1}")
                });
                first = false;
            }
        }
        p
    }

    /// 沿计划路线预测插值出机器人本帧世界坐标：从当前所在节点向路线下一节点推进，
    /// 进度由“距上次节点变更的时长 / 逐节点时长”确定，到 1.0 时下一拍心跳恰好到达，形成连续动画。
    fn anim_world(&self, r: &RobotState, now: u64) -> (f32, f32) {
        let cur = (r.location.x, r.location.y);
        let next = (|| {
            let cn = r.location.node?;
            let idx = r.route.iter().position(|&n| n == cn)?;
            let nxt = *r.route.get(idx + 1)?;
            let l = self.service.map.node_location(nxt)?;
            Some((l.x, l.y))
        })();
        match next {
            Some((nx, ny)) => {
                let elapsed = now.saturating_sub(r.last_change_ms) as f32;
                let t = (elapsed / NODE_MS).clamp(0.0, 1.0);
                (cur.0 + (nx - cur.0) * t, cur.1 + (ny - cur.1) * t)
            }
            None => cur,
        }
    }

    /// 将新帧机器人数据就地同步进常驻模型：逐行 `set_row_data`（保留元素实例，不销毁重建），
    /// 仅在数量变化时尾部 `push_back` / `remove`，从而不每帧替换 ModelRc 而丢失点击。
    fn sync_robots(&self, infos: Vec<RobotInfo>) {
        let m = &self.robots_model;
        let want = infos.len();
        let have = m.row_count();
        let mut it = infos.into_iter();
        for i in 0..have.min(want) {
            if let Some(v) = it.next() {
                m.set_row_data(i, v);
            }
        }
        for v in it {
            m.push(v);
        }
        while m.row_count() > want {
            m.remove(m.row_count() - 1);
        }
    }
}

impl Geo {
    /// 世界坐标 → 地面像素（先投影后适配）。本体绘制于 `(gx, gy - lift)`。
    #[inline]
    fn to_ground(self, x: f32, y: f32) -> (f32, f32) {
        let (u, v) = if self.iso {
            iso_project(x, y)
        } else {
            top_project(x, y)
        };
        (
            self.off_x + (u - self.min_u) * self.scale,
            self.off_y + (v - self.min_v) * self.scale,
        )
    }
}

/// 生成等距棋盘地板的三串 SVG 路径：浅色块/深色块（按 (col+row) 奇偶）+ 网格描边。
/// 每个整数字格子投影为一个闭合菱形（等距下为菱形，顶视下为正方形）。
fn floor_paths(geo: &Geo, map: &Map) -> (String, String, String) {
    let (min_x, min_y, max_x, max_y) = map.bounds().unwrap_or((0.0, 0.0, 0.0, 0.0));
    let mut light = String::new();
    let mut dark = String::new();
    let mut grid = String::new();
    for col in min_x.floor() as i32..max_x.ceil() as i32 {
        for row in min_y.floor() as i32..max_y.ceil() as i32 {
            let a = geo.to_ground(col as f32, row as f32);
            let b = geo.to_ground(col as f32 + 1.0, row as f32);
            let c = geo.to_ground(col as f32 + 1.0, row as f32 + 1.0);
            let d = geo.to_ground(col as f32, row as f32 + 1.0);
            let dia = format!(
                "M {:.1} {:.1} L {:.1} {:.1} L {:.1} {:.1} L {:.1} {:.1} Z ",
                a.0, a.1, b.0, b.1, c.0, c.1, d.0, d.1
            );
            grid.push_str(&dia);
            if (col + row).rem_euclid(2) == 0 {
                light.push_str(&dia);
            } else {
                dark.push_str(&dia);
            }
        }
    }
    (light, dark, grid)
}

// ============================ 通用辅助 ============================

#[inline]
fn s(v: impl Into<String>) -> SharedString {
    SharedString::from(v.into())
}

/// 同一节点上多机器人的抖动偏移（黄金角分布），避免完全重叠。
fn jitter(id: u64) -> (f32, f32) {
    let ang = (id as f32) * 2.399_963;
    (ang.cos() * 12.0, ang.sin() * 12.0)
}

fn status_code(s: RobotStatus) -> i32 {
    match s {
        RobotStatus::Idle => 0,
        RobotStatus::Busy => 1,
        RobotStatus::Charging => 2,
        RobotStatus::Error => 3,
        RobotStatus::Offline => 4,
    }
}

fn status_str(s: RobotStatus) -> &'static str {
    match s {
        RobotStatus::Idle => "空闲",
        RobotStatus::Busy => "忙碌",
        RobotStatus::Charging => "充电",
        RobotStatus::Error => "故障",
        RobotStatus::Offline => "离线",
    }
}

fn task_status_code(t: TaskStatus) -> i32 {
    match t {
        TaskStatus::Pending => 0,
        TaskStatus::Assigned => 1,
        TaskStatus::Executing => 2,
        TaskStatus::Completed => 3,
        TaskStatus::Failed => 4,
    }
}

fn task_status_str(t: TaskStatus) -> &'static str {
    match t {
        TaskStatus::Pending => "待处理",
        TaskStatus::Assigned => "已分配",
        TaskStatus::Executing => "执行中",
        TaskStatus::Completed => "已完成",
        TaskStatus::Failed => "失败",
    }
}

fn prio_str(p: app::domain::TaskPriority) -> &'static str {
    use app::domain::TaskPriority::*;
    match p {
        Low => "低",
        Normal => "中",
        High => "高",
        Urgent => "紧急",
    }
}

fn robot_row(r: &RobotState) -> String {
    let node = r
        .location
        .node
        .map(|n| format!("N{}", n.0))
        .unwrap_or_else(|| "-".into());
    let task = r
        .current_task
        .map(|t| format!("#{}", t.0))
        .unwrap_or_else(|| "-".into());
    let cargo = if r.carrying { " 📦" } else { "" };
    format!(
        "#{:<4} {:<3} {:>3}%  {:<6} {}{}",
        r.id.0,
        status_str(r.status),
        r.battery.0,
        node,
        task,
        cargo
    )
}

fn task_row(t: &Task) -> String {
    let robot = t
        .assigned_robot
        .map(|r| format!("#{}", r.0))
        .unwrap_or_else(|| "-".into());
    format!(
        "#{:<6} {:<3} {:<6} {}",
        t.id.0,
        prio_str(t.priority),
        task_status_str(t.status),
        robot
    )
}

// ============================ 折线图数据 ============================

/// 折线采样点序列：`(x 相对秒数, y 值)`。
type Series = Vec<(f64, f64)>;

/// 由采样历史构造三条曲线：心跳速率、分配速率、累计完成率（x 为相对起始秒数）。
fn build_series(samples: &[Sample]) -> (Series, Series, Series) {
    let t0 = samples.first().map(|s| s.t_ms).unwrap_or(0);
    let mut hb = Vec::new();
    let mut asg = Vec::new();
    let mut rate = Vec::new();
    for i in 0..samples.len() {
        let s = &samples[i];
        let x = s.t_ms.saturating_sub(t0) as f64 / 1000.0;
        if i > 0 {
            let prev = &samples[i - 1];
            let dt = s.t_ms.saturating_sub(prev.t_ms) as f64 / 1000.0;
            if dt > 1e-6 {
                hb.push((x, s.heartbeats.saturating_sub(prev.heartbeats) as f64 / dt));
                asg.push((x, s.assigned.saturating_sub(prev.assigned) as f64 / dt));
            }
        }
        let cr = if s.submitted > 0 {
            s.completed as f64 / s.submitted as f64 * 100.0
        } else {
            0.0
        };
        rate.push((x, cr));
    }
    (hb, asg, rate)
}

/// 将点序列映射到固定 CHART_W×CHART_H 绘图区，生成 SVG 折线路径字符串。
fn series_path(pts: &[(f64, f64)]) -> String {
    if pts.len() < 2 {
        return String::new();
    }
    let pad = 6.0f64;
    let x_max = pts.iter().map(|p| p.0).fold(1.0f64, f64::max).max(1.0);
    let y_max = pts.iter().map(|p| p.1).fold(1.0f64, f64::max).max(1.0);
    let w = CHART_W as f64;
    let h = CHART_H as f64;
    let mx = |x: f64| pad + (x / x_max) * (w - 2.0 * pad);
    let my = |y: f64| h - pad - (y / y_max) * (h - 2.0 * pad);
    let mut out = String::new();
    for (i, &(x, y)) in pts.iter().enumerate() {
        out.push_str(&if i == 0 {
            format!("M {:.1} {:.1}", mx(x), my(y))
        } else {
            format!(" L {:.1} {:.1}", mx(x), my(y))
        });
    }
    out
}

/// 折线图右上角显示的最新值标签。
fn last_label(pts: &[(f64, f64)]) -> String {
    match pts.last() {
        Some(&(_, y)) => format!("{:.1}", y),
        None => "-".to_string(),
    }
}

// ============================ 投影几何单测 ============================
#[cfg(test)]
mod tests {
    use super::*;

    /// 12x7 网格的世界坐标点。
    fn grid_pts() -> Vec<(f32, f32)> {
        let mut v = Vec::new();
        for x in 0..12 {
            for y in 0..7 {
                v.push((x as f32, y as f32));
            }
        }
        v
    }

    #[test]
    fn iso_project_definition() {
        // u = x - y, v = (x + y) / 2
        assert_eq!(iso_project(0.0, 0.0), (0.0, 0.0));
        assert_eq!(iso_project(4.0, 1.0), (3.0, 2.5));
        assert_eq!(iso_project(0.0, 1.0), (-1.0, 0.5));
    }

    #[test]
    fn iso_project_axis_directions() {
        // x 增大 → u 增大（一侧横向）；y 增大 → u 减小（另一侧横向），二者均使 v 增大。
        let (u_x, v_x) = iso_project(1.0, 0.0);
        let (u_y, v_y) = iso_project(0.0, 1.0);
        assert!(u_x > 0.0 && u_y < 0.0);
        assert!(v_x > 0.0 && v_y > 0.0);
    }

    #[test]
    fn iso_bbox_fits_canvas() {
        let geo = compute_geo(true, &grid_pts());
        for &(x, y) in &grid_pts() {
            let (gx, gy) = geo.to_ground(x, y);
            assert!(
                (MAP_PAD - 0.5..=MAP_W - MAP_PAD + 0.5).contains(&gx),
                "gx {gx} out of range"
            );
            assert!(
                (MAP_PAD - 0.5..=MAP_H - MAP_PAD + 0.5).contains(&gy),
                "gy {gy} out of range"
            );
        }
    }

    #[test]
    fn lift_monotonic_and_bounded() {
        assert_eq!(lift_for(0), 0.0);
        assert!((lift_for(100) - MAX_LIFT).abs() < 1e-3);
        assert!(lift_for(40) < lift_for(60));
        // 超界电量钉到上限
        assert!((lift_for(255) - MAX_LIFT).abs() < 1e-3);
        for b in 0..=255u8 {
            let l = lift_for(b);
            assert!((0.0..=MAX_LIFT + 1e-3).contains(&l));
        }
    }

    #[test]
    fn top_mode_is_axis_aligned() {
        let geo = compute_geo(false, &grid_pts());
        // 顶视下世界 x 增大→地面 x 单调增大，y 增大→地面 y 单调增大。
        let p0 = geo.to_ground(0.0, 0.0);
        let px = geo.to_ground(1.0, 0.0);
        let py = geo.to_ground(0.0, 1.0);
        assert!(px.0 > p0.0 && (px.1 - p0.1).abs() < 1e-3);
        assert!(py.1 > p0.1 && (py.0 - p0.0).abs() < 1e-3);
    }
}

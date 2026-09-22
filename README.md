# AGV/AMR 智能调度系统 (Robot Scheduling System)

一个面向 **AGV/AMR 机器人集群**的高性能调度系统骨架,附带 **2.5D 等距可视化监控面板**。
系统以"单决策者 + 事件驱动"的调度核心为中心,通过 TCP 长连接接入虚拟机器人集群,
实时完成任务分配、路径规划、冲突/死锁检测、低电量自动回充,并在 Slint GUI 上
以连续动画呈现机器人的行驶路线、取货送达过程与系统吞吐态势。

> 本项目定位为**可替换、可扩展的参考骨架**:调度策略、路径算法、UI 均可独立替换,
> 各层仅依赖领域模型,通过 channel 解耦。

---

## 目录

- [特性一览](#特性一览)
- [快速开始](#快速开始)
- [架构总览](#架构总览)
- [模块结构](#模块结构)
- [核心机制](#核心机制)
- [调度闭环数据流](#调度闭环数据流)
- [GUI 监控面板](#gui-监控面板)
- [运行配置](#运行配置)
- [测试与质量](#测试与质量)
- [技术栈](#技术栈)

---

## 特性一览

- **事件驱动调度器**:5ms tick,优先级老化防饥饿,超时/离线任务回收重试。
- **可插拔路径规划**:`Pathfinder` trait 抽象,内置基于二叉堆的 **A\***（欧氏启发）。
- **取货送达两段式任务**:`Transport { pickup, dropoff }`,机器人先空载驶往取货点、
  装卸停留后**载货**驶往放货点,全程逐节点连续行驶。
- **冲突与死锁可视化 + 自动解除**:检测拥挤节点、目的地争用与死锁环,
  对每个环"假释"一台优先级最低的牺牲者让路(带冷却窗口防抖)。
- **低电量自主回充**:机器人电量低于阈值时,**沿 A\* 路径逐节点开往**最近充电桩
  (而非瞬移),到站充电,充满后恢复空闲。
- **无锁高并发存储**:`DashMap` 分片 + 原子计数,支撑上千机器人高频心跳上报。
- **紧凑二进制协议**:4 字节长度前缀 + bincode 负载,相较 JSON 更小更快。
- **2.5D 等距 / 2D 顶视双模式 GUI**:自适应缩放、棋盘地板、悬浮阴影、电量高度编码、
  轨迹折线、计划路线、目的地旗帜、载货图标、吞吐曲线,点击机器人查看详情。
- **双运行形态**:GUI 监控面板(`app`)与无头日志演示(`headless`)。

---

## 快速开始

### 环境要求

- Rust **edition 2024** 工具链(`rustc 1.85+`)
- 运行 GUI 需要平台图形后端(Windows / macOS / Linux X11·Wayland)

### 构建与运行

```bash
# 构建整个工作区
cargo build

# 运行 GUI 监控面板（默认 bin = app）
cargo run

# 运行无头演示（纯日志跑一遍调度闭环，约 6 秒后打印统计并退出）
cargo run --bin headless

# 发布构建（opt-level=3 + thin LTO）
cargo build --release
```

GUI 启动后:

1. 在顶部「机器人数」选择集群规模,点击 **▶ 接入 / 重启集群** 拉起虚拟机器人;
2. 勾选 **⚡ 自动生成任务** 持续投喂取货送达任务,或点 **＋ 单发任务** 手动投递;
3. 点击地图上任意机器人查看其轨迹、路线、目的地与当前任务详情;
4. 用 **◇ 等距视图** 切换 2.5D / 2D;**📈 吞吐曲线**、**⚠ 冲突/死锁** 控制叠加层。

---

## 架构总览

系统自底向上分为领域、引擎、存储、传输、装配、仿真、可视化等层,层间以
channel 与 trait 解耦:

```
                        ┌──────────────────────────────┐
   GUI (Slint)  <────── │  main.rs  投影/动画/桥接      │
   headless.rs  ──────► ├──────────────────────────────┤
                        │  service.rs  装配 & 后台任务  │
                        ├───────────┬──────────────────┤
             事实来源    │ storage   │  engine          │  决策 & 寻路
           (DashMap) ◄─│ MemoryStore│  Scheduler       │
                        │  快照读写  │  A* / Conflict   │
                        ├───────────┴──────────────────┤
                        │  transport  TCP + bincode     │  接入机器人
                        ├──────────────────────────────┤
                        │  simulator  虚拟机器人集群     │  演示数据源
                        └──────────────────────────────┘
                                  domain  纯领域模型（零依赖）
```

- **单决策者**:所有"机器人↔任务"绑定决策只在调度器一个消费者内串行完成,
  跨表绑定天然无竞态,写入路径极短。
- **快照语义**:读接口返回可 `Clone` 的值快照,不向调用方泄漏 `DashMap` 守卫。
- **前后端同源**:GUI 与 headless 只是"读取同一份事实来源"的不同前端。

---

## 模块结构

```
src/
├── domain/          纯领域模型，不依赖其它层
│   ├── map.rs         Map / NodeId / Location / Edge / Path(SmallVec)
│   ├── robot.rs       RobotState / RobotStatus / Battery / Heartbeat（含 carrying）
│   └── task.rs        Task / TaskKind(Transport/Charge/Patrol) / 优先级 / 老化
├── engine/          调度核心（大脑）
│   ├── scheduler.rs   事件驱动调度器：回收→老化匹配→死锁解除
│   ├── pathfinding.rs Pathfinder trait + A* 实现
│   └── conflict.rs    拥挤/争用/死锁环分析
├── storage/
│   └── memory_store.rs  DashMap 无锁状态机 + 轨迹环形缓冲 + 原子统计
├── transport/
│   ├── codec.rs       长度前缀 + bincode 编解码；ClientMsg / ServerMsg 协议
│   └── server.rs      TCP 服务端、连接注册表、下发路由泵
├── service.rs       装配各组件并拉起后台任务（调度器/服务端/派发泵/指标采集）
├── simulator.rs     虚拟机器人：注册、逐节点行驶、取货载货、低电回充、心跳
├── metrics.rs       周期性采样吞吐历史（心跳/分配/完成率）
├── config.rs        SystemConfig 可调参数 + 进程级单调时钟
├── main.rs          Slint GUI：投影几何、动画插值、数据桥接、交互回调
└── bin/headless.rs  无头日志演示
ui/
└── scheduler.slint  声明式界面定义（地图、面板、曲线、控件）
build.rs             构建期编译 .slint
```

---

## 核心机制

### 调度器 (`engine/scheduler.rs`)

每 tick 依序执行:

1. **超时/离线回收**:分配后无进展或绑定机器人已离线的任务,按重试次数回 `Pending` 或判 `Failed`;
2. **老化匹配**:按"有效优先级"(每等待 `priority_aging_ms` 提升一级,封顶 `Urgent`)排序待分配任务,
   贪心为每个任务选**距取货点最近**且不在冷却窗口的空闲机器人,规划路线后落库并下发;下发通道拥塞则回滚留待下 tick;
3. **死锁解除**:对本帧全量快照跑冲突分析,对检出的每个死锁环假释一台牺牲者(有效优先级最低、平手取 id 最大者),并置入冷却窗口。

### 取货送达两段式 (`plan_task_route`)

对 `Transport { pickup, dropoff }`,调度器合并 `机器人→取货点→放货点` 两段路径
(衔接的取货点去重),**目的地取放货点**,并把取货节点随下发传给机器人。
机器人驶至取货点时停留若干拍模拟装卸,并把后续心跳标记为**载货**(`carrying`)。

### 低电量回充 (`simulator.rs`)

机器人电量低于 `Battery::LOW_THRESHOLD`(20%)时,用 A\* 求到最近充电桩的路径并
**逐节点驶回**(期间上报 `Charging` 心跳),到站后回升电量,充到 `FULL_THRESHOLD`(95%)恢复空闲。
关键约束:**任何位置变化都必须逐节点离散推进**,绝不瞬移,以保证轨迹与动画连续。

### 冲突与死锁 (`engine/conflict.rs`)

基于"占位节点 + 目的地"构造等待有向图,检出环即视为死锁;同时统计拥挤节点与目的地争用,
供 GUI 叠加标记与态势条展示。

---

## 调度闭环数据流

```
   提交任务 ──► SchedulerHandle ──(flume 事件)──► Scheduler.run
                                                      │ 老化排序 + A* 规划
                                                      ▼
                          MemoryStore.assign(Busy + route + target)   Dispatch{path,pickup}
                                                      │                        │
             机器人逐节点上报 Heartbeat ◄─────────────┘                        ▼
             (location/battery/status/carrying)                    dispatch_pump ──TCP──► 机器人
                                                      ▲                        （TaskAssign: task,path,pickup）
                          apply_heartbeat 刷新快照 ───┘
                                                      │
             机器人到达终点 TaskDone ──(flume)──► Scheduler.complete_task → Idle
```

GUI 侧以 ~30fps 定时器读取 `MemoryStore` 快照,沿 `route` 用
`(now - last_change_ms) / 每节点时长` 做**预测插值**,实现平滑行驶动画。

---

## GUI 监控面板

- **顶部指标**:在线 / 空闲 / 忙碌 / 待处理 / 执行中 / 已完成 / 心跳 / 连接数;
- **控制条**:集群规模、接入/断开、自动生成任务、单发任务、视图与叠加层开关;
- **中央地图**:2.5D 等距棋盘地板 + 贴地网格边 + 节点、充电桩、机器人(悬浮阴影、
  电量高度编码、载货货箱、死锁脉冲环)、选中机器人的轨迹与完整计划路线、目的地旗帜、冲突标记;
- **右侧列表**:机器人明细(状态/电量/所在节点/任务/载货)与任务看板;
- **底部曲线**:心跳速率、分配速率、累计完成率;
- **交互**:点击机器人弹出详情浮层(状态、电量、所在节点、目的地、轨迹采样数、当前任务)。

---

## 运行配置

`config::SystemConfig` 集中管理可调参数(默认值):

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `listen_addr` | `127.0.0.1:0` | TCP 监听地址(`:0` 由系统分配端口) |
| `scheduler_tick_ms` | `5` | 调度主循环 tick,决定分配延迟量级 |
| `heartbeat_timeout_ms` | `5000` | 超时未收心跳判定离线 |
| `task_timeout_ms` | `8000` | 分配后无进展的回收阈值 |
| `max_task_retries` | `3` | 任务回收重试上限,超过判失败 |
| `priority_aging_ms` | `4000` | 优先级老化步长(防饥饿) |
| `deadlock_cooldown_ms` | `1500` | 牺牲者假释后的让路冷却窗口 |
| `event_channel_capacity` | `65536` | 事件/下发有界通道容量(背压) |
| `enable_deadlock_resolution` | `true` | 关闭则仅检测可视化、不干预调度 |

日志级别通过 `RUST_LOG` 环境变量控制(`tracing-subscriber` env-filter),默认 `info`。

---

## 测试与质量

```bash
cargo test --workspace          # 单元测试（领域/调度/存储/GUI 投影/路径合并等）
cargo clippy --workspace --all-targets -- -D warnings   # 严格 lint（零告警门禁）
cargo fmt                       # 格式化
```

覆盖点示例:优先级老化封顶、`TaskKey` 排序(优先级 + FCFS)、牺牲者选取、
A\* 路径合并与衔接去重(`plan_task_route`)、等距/顶视投影几何、存储状态流转等。

---

## 技术栈

| 领域 | 选型 |
| --- | --- |
| 异步运行时 | `tokio`(full) |
| 通道 / 并发容器 / 锁 | `flume`、`crossbeam-channel`、`dashmap`、`parking_lot` |
| 序列化 | `serde`、`bincode`、`serde_json` |
| 数据结构 | `smallvec`(`Path`)、`bytes` |
| 协议框架 | `tokio-util`(codec)、`futures`、`byteorder` |
| 日志追踪 | `tracing`、`tracing-subscriber` |
| 错误处理 | `thiserror`、`anyhow` |
| 随机数 | `rand` |
| GUI | `slint` 1.x(声明式 + 命令式 Rust 桥接)、`slint-build` |

---

## 许可与说明

本项目为调度系统**核心骨架 / 参考实现**,内置的贪心匹配、A\*、死锁解除等策略均可替换:
更换为成本矩阵 + 匈牙利算法或拍卖算法时,通常仅需改动 `run_matching`;
更换路径算法时仅需实现 `Pathfinder` trait。

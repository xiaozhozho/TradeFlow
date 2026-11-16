# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概览

TradeFlow：Python 量化交易框架（OKX WebSocket 行情订阅 → DolphinDB 流数据存储 → 策略调度），以第三方包 `tradeflow` 形式提供，当前版本 3.5.0，要求 Python >= 3.10。项目文档、注释、日志均为中文。

- `tradeflow/` — 发布包（`pip install -e .`，setuptools 仅打包 `tradeflow*`）
- `strategy/` — 策略工作区，不随包发布（`example.py` 为模板，`check_stream_tables.py` 为落库链路检查脚本）
- `tests/` — 单元测试（离线）；`tests/integration/` 为 DolphinDB 真机集成测试（环境变量门控）
- `docs/superpowers/plans/` — 实施计划文档

## 常用命令

```bash
pip install -e .                                   # 安装（可编辑模式）
python -m strategy.example                         # 运行完整示例（需 DolphinDB + 网络；在项目根目录执行）
pytest -q                                          # 全部单元测试（无需网络与 DolphinDB）
pytest tests/test_schema.py -q                     # 单个测试文件
pytest tests/test_handlers_market.py::test_tickers -q   # 单个用例
python -m strategy.check_stream_tables --timeout 120    # 订阅→解析→落库全链路检查
```

DolphinDB 集成测试（**会 drop/recreate 全部流表**，必须让 `DOLPHINDB_HOST` 指向独立测试实例，不要对运行中策略的库执行）：

```bash
TRADEFLOW_TEST_DDB=1 TRADEFLOW_TEST_DDB_ALLOW_DROP=1 pytest tests/integration/test_stream_schema.py -q
TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_archive.py -q
```

pytest 配置在 `pyproject.toml`（`pythonpath = ["."]`）；无 pytest-asyncio，异步相关测试直接操作 `asyncio`/事件循环。

## 架构

### 运行模型与数据流

```
OKX WebSocket → Subscribe 基类（subscribe/base.py：收包/心跳/指数退避重连/动态订阅）
    → handlers.dispatch(packet) → list[Batch]（纯函数解析）
    → StreamWriter（独立线程 + 独立 DDB session，按表批量 tableInsert）
    → DolphinDB 流表（实时总线）
    → 流引擎（stream/engines.py）→ share* 快照表 / 指标表
    → Strategy.scan() 通过 Dolphin.runScript 读取
```

- 主线程：`Kernel.start()` 循环调用 `run()`（`risk.run()` + 各策略 `run()`），间隔 `pause` 秒。
- 守护线程：Kernel 自建 asyncio 事件循环，跑 4 条 WS 连接（public / business / private / business-private），由 `SubscribeManager` 编排、共享一个 `StreamWriter`。跨线程调度一律走 `asyncio.run_coroutine_threadsafe`。
- `StreamWriter` 队列满时丢弃并计数（`kernel.health()` 可见），绝不反压行情连接。

### Kernel / Strategy 生命周期

- `Kernel` 关键参数：`flag`（**'0' 实盘 / '1' 模拟盘，默认 '1' 模拟盘**；实盘启动会打印警告）、`pause`、`warmup`、`private`、`market`、`depth`、`options`、`algo`、`recreate_tables`、`max_errors`（策略连续异常停用阈值，默认 10）；`dolphin`/`api` 为离线测试的依赖注入入口。
- `kernel.addStrategy(cls)` 实例化策略并合并 `strategy.instIds` 到 `kernel.instIds`。
- `kernel.start()` → `init()`：`strategy.preinit()` → `prepare()`（按启用频道建流表 + 建引擎）→ `subscribe()` → 设置持仓模式 → `sleep(warmup)`。
- `Strategy.run()` 首次调用执行 `init()`（只执行一次），之后每次 `scan()`；策略逻辑写在 `scan()`。

### 两个"唯一来源"（改动入口）

- **频道清单**：`subscribe/channels.py` 的 `ChannelSpec`（endpoint / arg 类型 / 写入表 / 默认开关）。`Kernel.publicChannels` 等 property 由它生成；不要在其他地方手写频道列表。
- **表结构**：`stream/schema.py` 的 `TableSpec` 注册表。`script.py` 从它生成建表脚本；handler 与 StreamWriter 统一走 `TableSpec.normalize` 对齐列。旧表列顺序保持不变，新列只能追加末尾——改 schema 后需 `Kernel(recreate_tables=True)` 重建（流表是实时总线，重建不影响 `dfs://` 历史库）。

### 流引擎（stream/engines.py）

- `subscription`：流表 → `latestIndexedTable` 最新快照（`shareTickTable`、`shareOrderTable` 等）。
- `reactive`：`createReactiveStateEngine`，K 线指标（ma/macd/波动率等），**只喂 `candleConfirmStreamTable`（confirm=1）**。
- `timeseries`：`createTimeSeriesEngine`，tick 5 分钟活跃度（`shareTickIndicatorTable`，供 `Kernel.getTradeInstid()` 排序）。
- 输出列顺序（reactive：分组列→metrics；timeseries：时间列→分组列→metrics）为实测验证，勿改。

### 其余模块要点

- `subscribe/handlers/`：packet → Batch 纯函数，`channel_key` 把 `candle1m/5m` 等归一；`dispatch` 兜底异常——任何脏数据不能拖垮连接。数值空串统一走 `utils/format.py` 的 `as_float/as_int`。
- `subscribe/reconcile.py`：断线后 REST 对账（订单/持仓/账户）+ K 线缺口补齐。
- `subscribe/orderbook.py`：`books` 增量盘口序号校验。
- `subscribe/legacy.py`：旧实现，仅为兼容保留，新代码不依赖。
- `trade.py`：`kernel.trader` 交易便捷层——数量按 `lotSz` 向下取整并校验 `minSz`、价格按 `tickSz` 规整、自动生成 `clOrdId`；合约规格优先读 `productStreamTable`，未命中回退 REST 并缓存。`Strategy` 暴露 `buy/sell/close/getPosition(s)/getAccount` 薄封装。
- `simulation.py`：离线开发工具 `FakeDolphin`（pandas 表模拟 `runScript` 常见查询，未覆盖语句抛 `NotImplementedError`）与 `FakeApi`（记录下单/平仓调用）。配合 `Kernel(dolphin=..., api=...)` 注入，可不连 DolphinDB / OKX 跑完整策略。
- `stream/archive.py`：tick/trade 归档到 `dfs://market`。docstring 里列的 DolphinDB 写法约束均为 3.00.6 实测结论（如 COMPO 需两个子域、必须函数式 `createPartitionedTable`、handler 不能引用全局变量），改动前先读。
- `stream/dolphin.py`：`Dolphin` 是单例（`__new__` + `_initialized` 守卫），脚本执行加锁；其他线程必须用 `dolphin.newSession()` 而非共享 session。
- `tradeflow/__init__.py` 用 `__getattr__` 懒加载 `Kernel/Strategy/get_logger`，避免 `import tradeflow` 加载全部依赖；`OKX_FORCE_IPV4` 开启时在包导入时 patch `socket.getaddrinfo`（仅作用于 okx.com 域名）。

## 关键约定与易踩坑

- 时间语义：全库统一 **UTC-naive TIMESTAMP**（OKX 毫秒时间戳），不要写入本地时间。
- `Kernel.run()` 对风控与每个策略做异常隔离：异常只记录并跳过本轮，策略连续异常达 `max_errors` 自动停用（成功一次清零），计数在 `health()["strategies"]` / `health()["risk"]`。
- `prepare()` 的建表集合 = 核心表 ∪ 启用频道表 ∪ **全部流引擎 source/target 表**（引擎表不隶属任何频道，必须无条件创建，否则新库首次启动会在建引擎时失败）。
- `flag` 默认 `'1'`（模拟盘）；`'0'` 实盘在 Kernel 构造时打印警告。
- `recreate_tables=True` 会清空全部流表；重建前 Kernel 会先摘除流引擎订阅（活跃订阅会阻塞 `dropStreamTable`）。
- OKX WebSocket 统一 443 端口（8443 已于 2026-10-31 停用）。
- 版本号在 `pyproject.toml` 与 `tradeflow/__init__.py`（`__version__`）两处维护，发版时同步；README 的"版本记录"逐版本追加。
- 行为变化需同步更新 README（频道矩阵、参数表、版本记录是用户文档的一部分）。

## 配置

- 所有密钥/连接信息来自环境变量；`.env` 查找顺序：`TRADEFLOW_ENV_FILE` → 当前工作目录 → 包目录 → 仓库根目录。**真实环境变量优先，`.env` 不覆盖已有变量**。模板见 `.env.example`；`.env` 已 gitignore。
- WS 行为可用环境变量覆盖：`WS_RECV_TIMEOUT`(30) / `WS_HEARTBEAT_INTERVAL`(20) / `WS_RECONNECT_MAX`(30) / `WS_QUEUE_SIZE`(200000) / `WS_BATCH_SIZE`(200) / `WS_FLUSH_INTERVAL`(0.2)。
- 日志写入调用脚本同目录的 `log/<脚本名>.log`（如 `test.py` → `log/test.log`），`TRADEFLOW_LOG_DIR` 可覆盖根目录。

## 参考文档

- OKX V5 API 文档（频道、参数、错误码以官方为准）：https://www.okx.com/docs-v5/zh/
- DolphinDB 文档（建表、流引擎、SQL 语法以官方为准）：https://docs.dolphindb.cn/zh/about/ddb_intro.html

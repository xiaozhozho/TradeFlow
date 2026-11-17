# TradeFlow

![version](https://img.shields.io/badge/version-3.6.2-blue)
![python](https://img.shields.io/badge/python-%3E%3D3.10-blue)
![dolphindb](https://img.shields.io/badge/DolphinDB-3.00.6%2B-orange)
![okx](https://img.shields.io/badge/market-OKX%20V5-black)

Python 量化交易框架：**OKX 行情订阅 + DolphinDB 流计算 + 策略调度**。以第三方包 `tradeflow` 形式提供，让你专注于写交易逻辑，行情订阅、流表/流引擎管理、断线对账、交易执行等基础设施交给框架。

## 目录

- [项目简介](#项目简介)
- [核心特性](#核心特性)
- [架构概览](#架构概览)
- [环境要求](#环境要求)
- [安装](#安装)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [策略开发指南](#策略开发指南)
- [行情与数据管道](#行情与数据管道)
- [配置说明](#配置说明)
- [监控与运维](#监控与运维)
- [测试](#测试)
- [常见问题](#常见问题)
- [目录结构](#目录结构)
- [版本与升级](#版本与升级)
- [版本记录](#版本记录)
- [许可证](#许可证)

## 项目简介

TradeFlow 是一个面向数字货币量化交易的轻量级框架，以 OKX（WebSocket 行情 + REST 交易）和 DolphinDB（流数据表 + 流计算引擎）为内核。设计目标是**极简启动一个量化策略**：继承 `Strategy`、声明合约/周期/指标、实现 `scan()`，剩下的工作框架自动完成。

**适合谁使用**

- 个人或小团队量化交易者，希望在单机环境快速搭建实盘/模拟盘策略；
- 熟悉 Python 与 pandas、希望用 DolphinDB 处理实时行情的开发者；
- 需要多品种、多周期、tick 与 K 线混用，且要求秒级响应（1 秒级轮询）的趋势、均值回归、截面轮动类策略。

**适用场景**

- 单品种/多品种趋势与均值回归（如 MA 交叉）；
- 资金费率、基差、持仓量等衍生数据策略（数据默认订阅并落表）；
- 多周期联动（1m 信号 + 15m 过滤）、tick 活跃度选股；
- 策略离线单元测试（不连 DolphinDB、不连 OKX）。

**项目状态**：活跃开发中，当前版本 3.6.0。

## 核心特性

- **声明式策略**：`instIds`（合约）、`candles`（周期）、`indicators`（指标）三个类属性声明，框架自动完成订阅、建流表、建流引擎；
- **OKX 全频道覆盖**：公共行情、K 线、盘口、私有账户/持仓/订单、策略委托，共 20+ 频道，按需开启；
- **DolphinDB 流计算**：实时流表作为数据总线，响应式状态引擎计算 K 线指标（MA/EMA/MACD/RSI/ATR/BOLL 等），时间序列引擎计算 tick 活跃度；
- **快照表零计算读取**：`share*Table` 由 `latestIndexedTable` 自动维护，策略直接读最新状态；
- **交易便捷层**：数量/价格自动规整、`clOrdId` 自动生成、按保证金下单、单笔/持仓限额校验、批量下单与撤单；
- **工程韧性**：心跳与指数退避重连、断线 REST 对账、K 线缺口补齐、写队列满时丢弃不反压、策略异常隔离与自动停用、健康指标与数据时效；
- **离线开发**：`FakeDolphin` / `FakeApi` 依赖注入，策略可在无网络、无数据库环境下完整回归测试；
- **可回测数据**：tick/trade 可一键归档到 DolphinDB DFS（`dfs://market`）。

## 架构概览

```
OKX WebSocket（public / business / private / business-private 四类连接）
    → handlers 解析（packet → Batch 纯函数，脏数据不拖垮连接）
    → StreamWriter（独立线程 + 独立 DDB session，按表批量写入）
    → DolphinDB 流表（实时数据总线，保留 1~6 小时）
    → 流引擎（快照 / 响应式状态 / 时间序列）
    → share*Table 快照与指标表
    → Strategy.scan()（默认 1 秒轮询）→ kernel.trader 下单（OKX REST）
```

- **主线程**：`Kernel.start()` 循环执行风控与各策略 `run()`，间隔 `pause` 秒；
- **守护线程**：Kernel 自建 asyncio 事件循环，承载全部 WebSocket 连接；
- **写线程**：`StreamWriter` 独立批量写库，队列满时丢弃并计数，绝不阻塞行情；
- **三个"唯一来源"**：频道清单 [`subscribe/channels.py`](tradeflow/subscribe/channels.py)、表结构 [`stream/schema.py`](tradeflow/stream/schema.py)、默认流引擎 [`stream/engines.py`](tradeflow/stream/engines.py)，新增频道/表/引擎只改对应注册表。

## 环境要求

| 依赖 | 版本/说明 |
|---|---|
| Python | >= 3.10 |
| DolphinDB | 3.00.6+（本地或远程实例，默认 `localhost:8848`） |
| 网络 | 可访问 OKX WebSocket/REST；仅公共行情时无需 API Key |
| OKX 账户 | 交易需 API Key（模拟盘用 `OKX_*_SIMULATE` 一组密钥） |

## 安装

```bash
# 在项目根目录（本文件所在目录）执行
pip install -e .
```

也可以不安装，直接把项目根目录加入 `PYTHONPATH`：

```bash
export PYTHONPATH=/path/to/TradeFlow:$PYTHONPATH
```

## 快速开始

**第 1 步：配置环境变量**（仅交易/私有频道需要；纯公共行情可跳过）

```bash
cp .env.example .env   # 填写 OKX 密钥与 DolphinDB 连接信息
```

**第 2 步：跑通行情落库链路**（可选，用于确认环境正常）

```bash
python -m strategy.example                                    # 全景示例（公共行情）
python -m strategy.check_stream_tables --timeout 90           # 全链路落库检查
```

**第 3 步：编写第一个策略**

```python
from tradeflow import Kernel
from tradeflow.strategy import Strategy


class MyStrategy(Strategy):
    instIds = {"BTC-USDT-SWAP"}          # 交易合约
    candles = ("candle1m",)              # 需要的 K 线周期
    indicators = {"ma": [5, 20]}         # 声明式指标（框架自动计算）

    def preinit(self): ...
    def init(self): ...

    def scan(self):
        ind = self.getIndicators("candle1m", limit=3, instId="BTC-USDT-SWAP")
        if len(ind) >= 2 and ind.iloc[-1]["ma_5"] > ind.iloc[-1]["ma_20"]:
            self.buyByMargin("BTC-USDT-SWAP", 10)   # 按 10 USDT 保证金做多


kernel = Kernel(flag="1")  # '1' 模拟盘（默认），'0' 实盘（启动打印警告）
kernel.addStrategy(MyStrategy)
kernel.start()
```

完整的多合约实盘风格示例见 [`strategy/ma_cross.py`](strategy/ma_cross.py)：

```bash
python -m strategy.ma_cross
```

## 核心概念

| 概念 | 说明 |
|---|---|
| `Kernel` | 框架调度器：建表建引擎、启动订阅、轮询策略、统一交易上下文 |
| `Strategy` | 策略基类：声明 `instIds/candles/indicators`，实现 `preinit/init/scan` |
| 频道（Channel） | OKX WebSocket 订阅单元，唯一来源 `subscribe/channels.py` |
| 流表（Stream Table） | DolphinDB 实时数据总线，按频道写入，保留 1~6 小时 |
| 快照表（share*Table） | 每品种最新状态（tick/持仓/订单/指标），由流引擎维护 |
| 流引擎（Engine） | 快照同步、响应式指标计算、tick 时间序列窗口 |
| `kernel.trader` | 交易执行层：规整数量/价格、保证金换算、限额校验、批量/撤单 |
| `Reconciler` | 断线重连后的 REST 对账与 K 线缺口补齐 |

**Kernel 常用参数**

| 参数 | 默认 | 说明 |
|---|---|---|
| `flag` | `'1'` | `'1'` 模拟盘 / `'0'` 实盘（实盘启动打印警告） |
| `pause` | 1 | 策略轮询间隔（秒） |
| `warmup` | 320 | 启动预热秒数（等待行情/订阅就绪） |
| `private` | True | False 时只订阅公共行情，无需私有 API Key |
| `market` | None | 公共行情频道集合，None 用默认 9 个频道 |
| `depth` | None | 盘口频道：`"books5"` / `"bbo-tbt"` / `"books"` |
| `options` | False | 订阅期权频道（opt-summary / option-trades / estimated-price） |
| `algo` | False | 订阅策略委托频道（orders-algo / algo-advance） |
| `candles` | ("candle1m","candle5m") | K 线周期元组；策略类属性 `candles` 自动并入 |
| `recreate_tables` | False | prepare() 时删除重建流表（**会清空流表数据**，升级/开发用） |
| `max_errors` | 10 | 策略连续异常多少次后自动停用（成功一次即清零） |
| `archive` | False | 将 tick/trade 归档到 `dfs://market`（回测数据） |
| `max_order_margin` | None | 单笔保证金上限（USDT），None 不限 |
| `max_position_margin` | None | 单品种持仓保证金上限（USDT），None 不限 |
| `dolphin` / `api` | None | 依赖注入（离线测试用 `FakeDolphin` / `FakeApi`） |

## 策略开发指南

### 策略生命周期

```
preinit() → init()（仅一次）→ scan()（每 pause 秒循环）→ kernel.stop()
```

- `preinit()`：`prepare()` 建表建引擎之前执行，适合打印配置；
- `init()`：订阅就绪后执行一次，适合初始化状态；
- `scan()`：策略主逻辑，框架已用 try/except 隔离——单次异常只跳过本轮，连续异常达 `max_errors` 次自动停用该策略，不影响其他策略。

### 声明周期与指标

策略通过类属性声明所需周期与指标，`prepare()` 时框架自动完成订阅、建表、建流引擎：

```python
class MyStrategy(Strategy):
    instIds = {"BTC-USDT-SWAP"}
    candles = ("candle1m", "candle15m")                    # 多周期并入 Kernel 订阅
    indicators = {"ma": [5, 15], "rsi": [14], "boll": [(20, 2)]}
```

- 支持指标：`ma / ema / mstd / high / low / volma / macd / rsi / atr / boll`（模板见 [`stream/indicators.py`](tradeflow/stream/indicators.py)）；表达式只用 DolphinDB 内置滚动函数，不依赖 ta 插件；
- 自动生成：`indicator_<类名>StreamTable`（指标流表）+ `share<类名>IndicatorTable`（最新快照）+ `<类名>IndicatorEngine`（响应式引擎，keyColumn=instId+channel，一次覆盖该策略全部周期）；
- 指标列名规则：`ma_5`、`rsi_14`、`macd_12_26`、`boll_20_2_up/mid/dn`；
- 未声明 `indicators` 的策略读取框架默认指标表（ma5/10/20、ema12/26、macd、mstd20、volMa5、high20/low20），行为与旧版本一致。

### 数据访问

Strategy 内置查询方法（无需手写 DolphinDB SQL）：

| 方法 | 说明 |
|---|---|
| `getBars(channel, limit, instId=None)` | 最近 N 根已收盘 K 线（按品种分组、升序、去重） |
| `getTick(instId)` | 单品种最新 tick 快照（dict，无数据返回 `{}`） |
| `getIndicators(channel, limit, instId=None)` | 最近 N 行指标（自动选择默认/策略专属指标表） |
| `getBook(instId, source="books5", levels=5)` | 最新盘口快照 `{ts, bids, asks}` |
| `getOrders(instId=None, state=None)` | 订单快照列表（shareOrderTable） |
| `getPosition / getPositions / getAccount` | 持仓与资金快照 |

### 交易与风控

`kernel.trader` 对常用交易操作做薄封装，策略内直接调用：

```python
position = self.getPosition("BTC-USDT-SWAP")   # 快照，无持仓返回 None
if position is None:
    self.buyByMargin("BTC-USDT-SWAP", 10)      # 按 10 USDT 保证金做多
else:
    self.close("BTC-USDT-SWAP")                # 按快照字段市价平仓
```

- 数量按合约 `lotSz` 向下取整并校验 `minSz`；限价单价格按 `tickSz` 四舍五入；
- `buyByMargin/sellByMargin(instId, margin_usdt)`：张数 = 保证金 x 杠杆 / (价格 x 合约面值 ctVal)，下单前校验 `max_order_margin` / `max_position_margin`；
- 自动生成 `clOrdId`（字母数字，≤32 字符），便于与 orders 频道/快照表关联；
- 合约规格优先读 `productStreamTable`，未命中回退 REST `get_instruments` 并缓存；
- `kernel.trader.batchBuy/batchSell`（按 20 条/批）与 `cancel(instId, ordId|clOrdId)`；
- 返回 OKX REST 原始应答，`code == "0"` 表示请求被接受；
- 全局风控 `RiskManage` 负责杠杆校正与绝对止损/止盈（`LEVER` / `MAX_LOSS` / `MAX_PROFIT`），为账户级总闸，不区分策略。

### 离线开发与测试

不连 DolphinDB、不连 OKX 也能完整跑策略（[`tradeflow/simulation.py`](tradeflow/simulation.py)）：

```python
import pandas as pd
from tradeflow import Kernel
from tradeflow.simulation import FakeDolphin, FakeApi

tables = {
    "shareTickTable": pd.DataFrame([
        {"instId": "BTC-USDT-SWAP", "last": 100.0,
         "ts": pd.Timestamp("2026-01-01 00:00:00")},
    ]),
}
kernel = Kernel(flag="1", private=False, warmup=0,
                dolphin=FakeDolphin(tables), api=FakeApi())
kernel.addStrategy(MyStrategy)
kernel.run()          # 策略 init() + scan() 全流程离线执行
```

- `FakeDolphin` 用 pandas 表模拟 `runScript`，覆盖框架内置查询语句（裸表名 / select / exec count / exec max / context by 窗口 / top N 列）；不支持的语句抛 `NotImplementedError`（附原文），可按需扩展；
- `FakeApi` 的 `trade.orders / trade.closes / trade.batches / trade.cancels` 记录交易调用，便于断言；
- `Kernel(dolphin=..., api=...)` 为依赖注入入口，正常实盘/模拟运行无需使用。

## 行情与数据管道

### 频道矩阵

频道清单唯一来源：[`tradeflow/subscribe/channels.py`](tradeflow/subscribe/channels.py)。端点：`public` = `/ws/v5/public`，`business` = `/ws/v5/business`，`private` = `/ws/v5/private`（需登录），`business-private` = 业务端点私有频道（需登录）。

| 频道 | 端点 | 写入表 | 默认 |
|---|---|---|---|
| `tickers` | public | `tickStreamTable` | 开 |
| `trades` | public | `tradeStreamTable` | 开 |
| `open-interest` | public | `openinterestStreamTable` | 开 |
| `funding-rate` | public | `fundingrateStreamTable` | 开 |
| `mark-price` | public | `markpriceStreamTable` | 开 |
| `index-tickers` | public | `indextickStreamTable` | 开 |
| `liquidation-orders` | public | `liquidationorderStreamTable` | 开 |
| `status` | public | `statusStreamTable` | 开 |
| `instruments` | public | `productStreamTable` | 开 |
| `books5` / `bbo-tbt` / `books` | public | `bookStreamTable` | 关（`depth=` 开启） |
| `price-limit` | public | `pricelimitStreamTable` | 关 |
| `opt-summary` / `option-trades` / `estimated-price` | public | 对应流表 | 关（`options=True`） |
| `candle1m` / `candle5m` | business | `candleStreamTable` + `candleConfirmStreamTable` | 开 |
| `mark-price-candle*` / `index-candle*` / `trades-all` | business | 对应流表 | 关 |
| `account` / `positions` / `orders` / `balance_and_position` / `liquidation-warning` / `account-greeks` | private | 对应快照/流表 | 开（`private=True`） |
| `fills` | private | `fillsStreamTable` | 关（VIP4+） |
| `orders-algo` / `algo-advance` | business-private | `algoOrderStreamTable` + `shareAlgoOrderTable` | 关（`algo=True`） |

任意 `candle*` 频道（`candle15m`、`candle1H` 等）复用同一表结构，多周期只需在 `candles` 中声明。

### 流数据表与流引擎

- 表结构唯一来源：[`stream/schema.py`](tradeflow/stream/schema.py)（`TableSpec`）；旧表列顺序不变，新列追加末尾；
- 快照表（`latestIndexedTable`）：`shareTickTable`、`shareCandleTable`、`shareOrderTable`、`shareAlgoOrderTable`、`shareAccountTable`、`sharePositionTable`、`shareBalanceTable`、`shareWarningTable`、`shareAccountGreeksTable`、`shareCandleIndicatorTable`、`shareTickIndicatorTable`；
- K 线指标引擎：`createReactiveStateEngine` 只喂 `candleConfirmStreamTable`（`confirm=1`），输出 ma5/ma10/ma20/ema12/ema26/macd/mstd20/volMa5/high20/low20；
- tick 活跃度引擎：`createTimeSeriesEngine` 5 分钟窗口/1 分钟步长，输出 count5m/open5m/high5m/low5m/close5m/avgPx/vol5m（供 `Kernel.getTradeInstid()` 排序）；
- `prepare()` 的建表集合 = 核心表 ∪ 启用频道表 ∪ 全部流引擎输入/输出表，新库首次运行无需手工建表。

### 动态订阅

不重启进程增删订阅：

```python
kernel.manager.subscribe("public", [{"channel": "tickers", "instId": "ETH-USDT-SWAP"}])
kernel.manager.unsubscribe("public", [{"channel": "tickers", "instId": "ETH-USDT-SWAP"}])
```

### 历史归档

[`tradeflow/stream/archive.py`](tradeflow/stream/archive.py) 将 tick/trade 落 `dfs://market`（`date(ts)` + `hash(instId)` 复合分区），供回测使用：

```python
kernel = Kernel(archive=True)          # 方式一：Kernel 参数一键启用（prepare 时生效）
# 方式二：手动幂等调用
from tradeflow.stream.archive import ensure_archive
ensure_archive()
```

## 配置说明

- 所有密钥/连接信息来自环境变量；`.env` 查找顺序：`TRADEFLOW_ENV_FILE` 指定路径 → 当前工作目录 → 包目录 → 源码仓库根目录；
- 真实环境变量优先级最高，`.env` 不会覆盖已存在的环境变量；
- 配置模板见 [`.env.example`](.env.example)，复制为 `.env` 后填写（`.env` 已 gitignore，请勿提交）；
- 主要变量：`OKX_API_KEY/SECRETE_KEY/PASSPHRASE`（实盘）、`OKX_*_SIMULATE`（模拟盘）、`DOLPHINDB_HOST/PORT/USER/PASSWORD`、`LEVER`（杠杆，默认 30）、`MAX_LOSS/MAX_PROFIT`（全局止损/止盈比例）；
- 日志默认写入调用脚本同目录的 `log/` 下，文件名为脚本名（如 `test.py` → `log/test.log`）；可用 `TRADEFLOW_LOG_DIR` 覆盖日志根目录；
- WebSocket 行为可用环境变量覆盖：`WS_RECV_TIMEOUT`（30s）、`WS_HEARTBEAT_INTERVAL`（20s）、`WS_RECONNECT_MAX`（30s）、`WS_QUEUE_SIZE`（200000）、`WS_BATCH_SIZE`（200）、`WS_FLUSH_INTERVAL`（0.2s）。

## 监控与运维

一条命令查看全部运行状态：

```python
import json
print(json.dumps(kernel.health(), indent=2, ensure_ascii=False))
```

返回内容：

- 各连接（public / business / private / algo）：`connected / last_message_at / reconnect_count / error_count / channels`；
- 写线程：`queue / written / dropped / errors`；
- `risk.errors`：风控累计异常次数；
- `strategies`：各策略连续异常次数与停用状态；
- `freshness`：tick 与 1m K 线最新数据距今秒数（2 秒缓存）——行情停摆可第一时间发现。

运行期保障：心跳保活、指数退避重连、断线后 REST 对账（订单/持仓/账户快照覆盖）与 K 线缺口补齐、刷盘失败分块重试，均为自动完成。

## 测试

```bash
# 单元测试（无需网络与 DolphinDB，覆盖解析/写库/对账/指标/交易/策略）
pytest -q
pytest tests/test_ma_cross.py -q                     # 单个文件
pytest tests/test_handlers_market.py::test_tickers -q  # 单个用例

# DolphinDB 集成测试（会 drop/recreate 全部流表，
# 必须指向独立测试实例，不要对正在运行策略的库执行）
TRADEFLOW_TEST_DDB=1 TRADEFLOW_TEST_DDB_ALLOW_DROP=1 pytest tests/integration/test_stream_schema.py -q
TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_archive.py -q
```

## 常见问题

**Q：必须同时安装 DolphinDB 吗？**
是。行情总线、指标计算、快照都在 DolphinDB 内完成。离线策略单测可用 `FakeDolphin` 免安装调试。

**Q：纯看行情、不下单，需要 OKX 密钥吗？**
不需要。`Kernel(private=False)` 仅订阅公共频道；交易与私有频道（账户/持仓/订单）才需要密钥。

**Q：如何切换模拟盘/实盘？**
`Kernel(flag='1')` 模拟盘（默认）、`flag='0'` 实盘。实盘启动会打印警告日志；模拟盘使用 `.env` 中 `OKX_*_SIMULATE` 一组密钥。

**Q：首次启动报"表不存在/建引擎失败"？**
3.5.0 起 `prepare()` 会自动创建流引擎依赖的全部表，新库直接启动即可；从旧版本升级且表结构有变化时，用 `Kernel(recreate_tables=True)` 重建（流表为实时总线数据，重建不影响 `dfs://` 历史库）。

**Q：支持哪些合约与周期？**
合约填 OKX `instId`（U 本位永续如 `BTC-USDT-SWAP`，现货/交割同理）；周期任意 `candle*` 频道（`candle1m/5m/15m/30m/1H/4H/1D` 等），在 `candles` 中声明即可。

**Q：策略报错了会怎样？**
单个策略异常只跳过当轮并记录日志，连续异常达到 `max_errors`（默认 10）次自动停用该策略，其他策略继续运行；状态在 `kernel.health()["strategies"]` 中查看。

**Q：多个策略能同时跑吗？**
可以，`kernel.addStrategy()` 多次添加即可，异常互相隔离。注意全局风控 `RiskManage` 是账户级总闸，止损止盈不区分策略归属。

**Q：行情断了怎么办？**
框架自动重连（指数退避）、私有频道重连后 REST 对账、业务端点重连后补齐 K 线缺口，无需人工干预；是否异常看 `health()["freshness"]` 与各连接 `reconnect_count/error_count`。

**Q：流表数据保留多久？能回测吗？**
流表为实时总线，默认保留 1~6 小时（按表配置）；历史数据用 `Kernel(archive=True)` 归档到 `dfs://market`。框架不内置回测引擎，归档数据可直接用于 DolphinDB 内回测或导出。

**Q：为什么 `ta::rsi` 之类的函数不可用？**
DolphinDB 3.00.6 不含 ta 扩展；声明式指标基于内置滚动函数 + `prev()` 实现（RSI/ATR/BOLL 均验证可用），无需插件。

**Q：如何新增框架不支持的指标？**
在 [`stream/indicators.py`](tradeflow/stream/indicators.py) 的 `_TEMPLATES` 中增加"指标名 → (DolphinDB 表达式, 参数个数)"即可，表达式只要求能用于响应式状态引擎。

## 目录结构

```
TradeFlow/
├── tradeflow/                  # 框架包（发布为第三方包 tradeflow）
│   ├── okx/                    # OKX SDK（同步上游 python-okx）
│   ├── subscribe/              # WebSocket 行情订阅模块
│   │   ├── base.py             # 连接/心跳/重连/动态订阅基类
│   │   ├── channels.py         # 频道清单唯一来源（端点/参数/写入表/默认开关）
│   │   ├── handlers/           # 报文解析（packet -> Batch 纯函数）
│   │   ├── public.py           # 公共行情与业务端点连接
│   │   ├── private.py          # 私有连接（需登录）与策略委托连接
│   │   ├── manager.py          # 多连接编排 + 共享 StreamWriter
│   │   ├── writer.py           # 后台批量写 DolphinDB
│   │   ├── reconcile.py        # 断线 REST 对账 + K 线缺口补齐
│   │   └── orderbook.py        # 增量盘口与序号校验（books）
│   ├── stream/                 # DolphinDB 连接、表结构与引擎
│   │   ├── schema.py           # 流表/快照表结构唯一来源（TableSpec）
│   │   ├── engines.py          # 流引擎定义（快照/响应式状态/时间序列）
│   │   ├── indicators.py       # 策略声明式指标（动态表/引擎生成）
│   │   ├── archive.py          # 流表 -> DFS 历史归档
│   │   ├── script.py           # 建表脚本（由 schema.py 生成）
│   │   └── dolphin.py          # DolphinDB 连接与执行
│   ├── utils/                  # 日志、网络、邮件、数据格式化工具
│   ├── kernel.py               # 核心调度组件
│   ├── strategy.py             # 策略基类
│   ├── trade.py                # 交易便捷层（数量/价格规整 + 下单/平仓）
│   ├── simulation.py           # 离线开发工具（FakeDolphin / FakeApi）
│   ├── risk.py                 # 风险管理
│   ├── indicator.py            # 指标计算
│   └── setting.py              # 全局配置
├── strategy/                   # 量化策略工作区（不随包发布）
│   ├── example.py              # 策略模板（行情落库链路）
│   ├── ma_cross.py             # MA5/MA15 交叉多合约模拟盘策略示例
│   └── README.md
├── tests/                      # 单元测试 + DolphinDB 集成测试
├── .env.example                # 环境变量模板
├── pyproject.toml              # 打包配置
├── requirements.txt
└── README.md
```

## 版本与升级

升级前建议先读对应版本的升级说明；完整历史见[版本记录](#版本记录)。所有小版本均保持向后兼容（破坏性变更会在升级说明中显式标注）。

### 从 3.5.0 升级

- 新增多周期与声明式指标：`Kernel(candles=...)`、`Strategy.candles/indicators`；未声明时行为与 3.5.0 完全一致（默认仍订阅 1m/5m、读默认指标表）；
- 新增 `Strategy.getBars/getTick/getIndicators/getBook/getOrders` 与 `buyByMargin/sellByMargin`；`kernel.trader` 增加 `sizeByMargin/lastPrice/batchBuy/batchSell/cancel`；
- `Kernel` 新增 `max_order_margin / max_position_margin / archive` 参数；
- `health()` 新增 `freshness`（tick 与 1m K 线最新数据距今秒数）；
- `prepare()` 建引擎改为按注册表当前内容创建（含策略声明生成的自定义引擎）。

### 从 3.4.0 升级

- `Kernel` 的 `flag` 默认值改为 `'1'`（模拟盘）；依赖"默认实盘"的调用需显式传 `flag='0'`；
- 新库首次运行无需手工建全表：`prepare()` 会自动创建流引擎依赖的全部表（此前指标表/策略委托表不在建表集合内，新环境首次启动会在建引擎时报错）；
- `Kernel.run()` 增加异常隔离：单策略异常不再终止进程，连续异常达到 `max_errors`（默认 10）次自动停用，其余策略继续运行；
- 新增 `kernel.trader` 与 `strategy.buy/sell/close/...`、`tradeflow.simulation` 离线工具，均为增量接口，旧代码不受影响。

### 从 3.3.x 升级

- 表结构变更（时间列统一 TIMESTAMP、`fundingrateStreamTable` 等追加列、新增 14 张表）：首次运行请用 `Kernel(recreate_tables=True)` 重建流表；流表数据为实时总线数据（保留 1~6 小时），重建不影响 `dfs://` 历史库；
- 时间语义统一为 UTC-naive：与旧版本写入的本地时间数据混用的窗口内，指标可能有短暂偏差，重建流表即可消除；
- 断线重连后自动进行 REST 对账（订单/持仓/账户）与 K 线缺口补齐，无需人工干预。

## 版本记录

### version 3.6.2

- 修复 Python 3.12 下运行策略时的废弃告警（行为不变）：
  `okx/utils.py` 的 `datetime.utcnow()` 改为带时区 `now()` 后去 tzinfo（ISO+Z 格式不变）；
  `okxclient` 的 POST 请求体由 `data=` 改为 httpx 推荐的 `content=`（线上字节与请求头不变）；
- `Api` 显式向各 API 类传 `use_server_time=None`，消除每次构造 Kernel 时的废弃告警
  （此前来自 `CopyTradingAPI` 的 `False` 默认值）；
- 新增 `tests/test_okx_compat.py` 防回归；okx 本地补丁记录于 `okx/UPSTREAM.md`。

### version 3.6.1

- 清理历史遗留：删除 `subscribe/legacy.py`（413 行）与 `utils/method.py`（历史数据下载脚本）；
  `Subscribe` 保留为 `base.Subscribe` 的旧名别名（计划 3.7 移除），
  构造 Kernel 不再附带创建多余的 REST 客户端；
- 日志统一：DolphinDB / 归档 / 邮件模块的 print 输出改为 logger（写入脚本日志文件）；
- `RiskManage` 杠杆校正增加 30 秒退避：设置失败时不再逐秒重试刷 REST；
- 删除死代码（`Dolphin.getSession`、Strategy 未使用导入），移除 tqdm 依赖。

### version 3.6.0

- 多周期一等支持：`Kernel(candles=...)` 与 `Strategy.candles` 声明式周期（任意 `candle*` 频道，15m/1h 等直接可用；指标引擎按 instId+channel 自动分组）；
- 声明式指标：`Strategy.indicators = {"ma": [5, 15], "rsi": [14], ...}`，`prepare()` 按策略自动生成指标流表/快照表/响应式引擎（表达式仅用 DDB 内置滚动函数 + prev()，实测 ta 扩展不可用）；
- 策略数据访问 API：`getBars / getTick / getIndicators / getBook / getOrders`；
- 保证金下单：`buyByMargin / sellByMargin` 与 `trader.sizeByMargin/lastPrice`，下单前校验 `max_order_margin / max_position_margin`；
- Trader 批量下单（20/批）、撤单 `cancel`；
- `Kernel(archive=True)` 一键接入 tick/trade 归档到 `dfs://market`；
- `health()` 新增 `freshness` 数据时效（tick / 1m K 线距今秒数，2s 缓存）；
- 策略示例 `strategy/ma_cross.py` 改为声明式指标 + 保证金下单。

### version 3.5.0

- 修复新库首次 `prepare()` 失败：建表集合改为 核心表 ∪ 启用频道表 ∪ 全部流引擎输入/输出表（此前指标表、策略委托表不在其中，建引擎时因表不存在而报错）；
- `Kernel.run()` 增加异常隔离：单策略/风控异常只记录并跳过本轮，策略连续异常达到 `max_errors`（默认 10）次自动停用；`kernel.health()` 新增 `risk.errors` 与 `strategies`（连续异常次数/停用状态）；
- `Kernel` 的 `flag` 默认值改为 `'1'`（模拟盘），`flag='0'` 实盘启动时打印警告；
- 新增 `tradeflow/trade.py` 交易便捷层：`kernel.trader` 及 `Strategy.buy/sell/close/getPosition/getPositions/getAccount`；数量按 `lotSz` 向下取整并校验 `minSz`、价格按 `tickSz` 规整、自动生成 `clOrdId`，合约规格优先读 `productStreamTable`，未命中回退 REST 并缓存；
- 新增 `tradeflow/simulation.py` 离线开发工具：`FakeDolphin`（pandas 表模拟查询）与 `FakeApi`（记录下单调用）；`Kernel(dolphin=..., api=...)` 支持依赖注入，策略可在不连 DolphinDB / OKX 的情况下完成 init+scan 回归测试。

### version 3.4.0

- `subscribe.py` 重构为独立 `subscribe` 模块：`base`（连接/心跳/指数退避重连/动态订阅）、`channels`（频道清单唯一来源）、`handlers`（报文解析纯函数）、`manager`（多连接编排）、`writer`（独立线程批量写库）、`reconcile`（断线 REST 对账 + K 线缺口补齐）、`orderbook`（增量盘口序号校验）；
- 默认公共频道扩展为 tickers/trades/funding-rate/mark-price/index-tickers/open-interest/liquidation-orders/status/instruments；K 线默认 1m/5m；
- 新增 `Kernel` 参数 `market/depth/options/algo/recreate_tables` 与 `kernel.health()` 健康指标；
- DolphinDB 流表结构统一由 `stream/schema.py` 管理（34 张流表/快照表），时间列统一 UTC-naive TIMESTAMP；
- 新增流引擎：`candleConfirmStreamTable`（只喂 confirm=1）+ 响应式状态引擎（MA/MACD/波动）、时间序列引擎（tick 5 分钟活跃度）、7 个最新快照动作；
- 新增 `stream/archive.py` 将 tick/trade 归档到 `dfs://market`（date+hash 复合分区，实测验证）；
- `RiskManage` 改用 `getPositions()` 并以 `_closing` 集合抑制重复平仓单；
- 依赖升级：pandas>=2.2、websockets>=13、SQLAlchemy>=2.0。

### version 3.3.2

- `Kernel` 新增 `warmup`（启动预热秒数）与 `private`（是否订阅私有频道）参数，支持仅公共行情运行；
- 修复 `Strategy.run()` 重复执行 `init()` 的问题，策略初始化只执行一次；
- WebSocket 消息解析由 `eval` 改为 `json.loads`，避免 JSON 布尔值导致断线重连；
- 私有频道数值字段统一使用 `pd.to_numeric` 容错（空串、`unsupported`、`off` 等按 0 处理），并按账户表结构对齐列；
- `Kernel.stop()` 取消订阅协程并优雅关闭 WebSocket；
- `strategy/example.py` 跑通完整示例：创建 DolphinDB 流表/引擎、订阅 OKX tick 并落库、每 3 秒统计 `tickStreamTable` 数据量。

### version 3.3.1

- 日志改为按调用脚本定位：在脚本同目录创建 `log/`，并写入与脚本同名的日志文件（如 `test.py` → `log/test.log`）；
- `get_logger` 支持从 `tradeflow` 顶层直接导入；
- `TRADEFLOW_LOG_DIR` 仍可用于覆盖日志根目录。

### version 3.3.0

- 包代码统一迁移到 `tradeflow/` 目录，可通过 `pip install -e .` 以第三方包导入；
- 新增 `strategy/` 量化策略开发目录与最小策略模板 `strategy/example.py`；
- 新增 `pyproject.toml` 打包配置与 `.env.example` 环境变量模板；
- `.env` 支持从当前目录、包目录、仓库根目录查找，并可用 `TRADEFLOW_ENV_FILE` 指定；
- 日志目录支持 `TRADEFLOW_LOG_DIR` 覆盖，源码模式下仍写入仓库根目录 `log/`。

### version 3.2.0

- okx 模块同步上游 python-okx 0.4.4（commit 06bb8a9），保留目录名 `okx`；
- 新增 `okx/api.py` 聚合层，适配上游拆分式 API 类（新增 Affiliate/CopyTrading/DualInvest/SpreadTrading）；
- WebSocket 地址迁移至 443 端口（OKX 已于 2026-10-31 停用 8443），并处理 `notice` 服务通知；
- 修复 pandas 3 兼容性：`applymap` 替换为 `dfApply`，避免字符串 dtype 下 `iloc` 回写数值报错；
- 新增 `OKX_FORCE_IPV4` 开关（默认开启，仅对 OKX 域名使用 IPv4；IPv6 正常的环境可设为 `0` 关闭）；
- 日志目录改为相对项目根目录并自动创建，不再依赖启动时的工作目录。

### version 3.1.1

- 修复 log.py 在 Linux 和 win32 效果差异不同的问题

### version 3.1.0

- 将 RiskManage 添加进主进程当中，而非与 subscribe 处于相同的子线程，提高线程安全

### version 3.0.0

- 添加 risk 模块用于全局风险控制，重要功能包括，调节持仓模式、保证金模式、杠杆、亏损时按照市价平仓等

### version 2.6.0

- 设计新的内存数据表：其中 shareTickTable 用于保存最新 tick 数据，shareAccountTable 用于保存账户最新信息，sharePositionTable 用于保存最新持仓情况

### version 2.5.0

- 修改 TradeFlow 模块文件结构；
- 修改 public、private 异步订阅，将其写入独立的子线程，守护线程开启；
- 修改 dolphin.py，将功能封装进 Dolphin 类，进行统一管理，添加 kernel 绑定；
- 添加 mysql.py 到 database 模块；
- 添加 email.py 到 utils 模块；
- 添加 chart.py 到 utils 模块

### version 2.0.0

- 添加创建流数据表的脚本、函数功能；
- 为 PublicSubscribe, PrivatePublic 添加 callback 函数，将订阅的数据插入流数据表；
- 添加 TradeFlow 的核心组件 kernel；
- 添加 TradeFlow 的核心组件----策略模版；
- 修改订阅形式，将 public、private 订阅函数使用 asyncio 封装成异步订阅。

### version 1.0.0

- 添加基本组件：log, utils, setting, decorator
- 封装 okx 为 subscribe，包括 public、private 订阅；
- 编写公共组件，包括 database, utils, setting。

## 许可证

本项目暂未声明开源许可证（仓库中没有 LICENSE 文件）。在未明确授权前，默认保留所有权利；如需对外开源或分发，请先补充 `LICENSE` 文件并更新本节说明。

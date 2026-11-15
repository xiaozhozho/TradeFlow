# TradeFlow 行情订阅与 DolphinDB 流数据升级实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在不改变 `Kernel → Strategy → Dolphin` 主框架的前提下，把 `tradeflow/subscribe.py` 重构为独立订阅模块，补齐 P0/P1 频道、异步批量落库、断线对账、动态订阅和 DolphinDB 流表/流引擎，解决“当前只能看行情、不能稳定支撑实盘策略”的问题。

**Architecture:** WebSocket 连接只负责“收包 + 解析 + 入队”，独立写线程按表批量写 DolphinDB；DolphinDB 用普通流表做实时总线，用 `latestIndexedTable` 做最新快照，用 `createReactiveStateEngine` / `createTimeSeriesEngine` 做指标计算；`Kernel` 只负责编排，不把行情逻辑写进策略。

**Tech Stack:** Python 3.10+、`websockets`、`pandas`、DolphinDB Python API 3.0.6+、pytest、pytest-asyncio。

---

## 1. 范围与边界

### 1.1 本计划要解决的问题

对应上一轮分析中的“五、对量化策略的实际影响”：

| 策略类型 | 目标状态 | 本计划对应能力 |
|---|---|---|
| 单品种、低频趋势/均值回归 | 稳定可用 | ticker、1m/5m K 线、未完成 K 线、断线补齐 |
| 多品种截面/轮动 | 稳定可用 | 动态订阅、批量落库、tick 5 分钟活跃度引擎 |
| 资金费率/基差/期现套利 | 稳定可用 | `funding-rate`、`mark-price`、`index-tickers`、`open-interest` 默认订阅 |
| 做市/盘口套利 | 可用（P1） | `books5`、`bbo-tbt`、`trades`；`books` 增量盘口单独一阶段 |
| 期权波动率/组合 Greeks | 可用（P1） | `opt-summary`、`option-trades`、`account-greeks`、`estimated-price` |
| 止盈止损/条件单/冰山/TWAP | 可用（P1） | `orders-algo`、`algo-advance` 推送 + 最新策略单快照 |
| 7×24 实盘自动交易 | 风险可控 | 心跳、指数退避重连、订单/持仓对账、K 线补齐、健康指标 |

### 1.2 明确不做

- 不重写 `Kernel`、`Strategy`、`RiskManage` 的公开用法；只增加可选参数和内部编排。
- 不把上游 `okx/websocket/` 整体嵌进来；继续使用项目自研订阅层，避免与 `subscribe` 重复。
- 不在本阶段实现期权做市、组合保证金、跨币种保证金等复杂业务逻辑。
- 不默认开启 `books` 增量盘口；先把快照型 `books5` / `bbo-tbt` 做稳。
- 不把历史行情全部塞进流表；流表只做实时总线，历史落 `dfs://` 分区表。

### 1.3 兼容性承诺

以下用法保持不变：

```python
from tradeflow import Kernel
from tradeflow.subscribe import PublicSubscribe, CandleSubscribe, PrivateSubscribe

kernel = Kernel(flag="1", pause=1, warmup=3, private=False)
kernel.addStrategy(ExampleStrategy)
kernel.start()
```

`tradeflow/subscribe.py` 会变成 `tradeflow/subscribe/` 包，但 `subscribe/__init__.py` 继续导出 `PublicSubscribe`、`CandleSubscribe`、`PrivateSubscribe`、`Subscribe`，因此旧导入不破坏。

---

## 2. 目标结构

```text
tradeflow/
├── subscribe/                    # 新增：独立行情订阅模块
│   ├── __init__.py               # 兼容导出 + SubscribeManager
│   ├── base.py                   # 连接、登录、心跳、重连、动态订阅
│   ├── channels.py               # 频道清单：端点/参数/表/默认开关
│   ├── handlers/
│   │   ├── __init__.py           # dispatch(packet) -> list[Batch]
│   │   ├── market.py             # 公共行情解析
│   │   └── private.py            # 账户/订单/持仓解析
│   ├── manager.py                # 多连接编排、启停、健康指标
│   ├── reconcile.py              # 断线后 REST 对账 + K 线补齐
│   ├── writer.py                 # 队列 + 批量写 DolphinDB
│   ├── public.py                 # PublicSubscribe / CandleSubscribe
│   └── private.py                # PrivateSubscribe / BusinessPrivateSubscribe
├── stream/
│   ├── schema.py                 # 新增：表结构唯一来源（TableSpec）
│   ├── script.py                 # 保留常量名，改为 schema/engine 生成
│   ├── engines.py                # 新增：流引擎定义与创建脚本
│   ├── archive.py                # 新增（P1）：流表 -> DFS 历史表
│   └── dolphin.py                # 增加幂等建表、独立 session、线程安全
├── utils/
│   ├── __init__.py               # 统一导出格式化函数
│   ├── format.py                 # 新增：所有订阅数据格式化纯函数
│   └── method.py                 # 保留旧函数，改为从 format 重新导出
├── kernel.py                     # 只改编排：prepare/subscribe/stop/查询
└── strategy.py                   # 只改指标查询的默认频道参数
```

设计原则：

1. **频道清单唯一来源**：`channels.py` 决定“订阅什么、写到哪张表”；`Kernel` 不再手写频道列表。
2. **表结构唯一来源**：`stream/schema.py` 决定列名、列类型、主键、时间列、容量；`script.py` 不再散落 20 份手写脚本。
3. **解析与 I/O 分离**：`handlers/` 只做 `packet -> DataFrame`，可单测；`writer.py` 只做批量写库。
4. **连接异常不外溢**：单频道错误、单条脏数据都不能拖垮整条连接。
5. **先对账再交易**：私有连接重连后先 REST 拉取未成交订单/持仓/账户，再让策略继续跑。

---

## 3. 频道设计

### 3.1 频道矩阵

`endpoint` 取值：`public` = `/ws/v5/public`，`business` = `/ws/v5/business`，`private` = `/ws/v5/private`，`business-private` = `/ws/v5/business` 且需要登录。

| 频道 | endpoint | 需要登录 | 参数 | 写入表 | 优先级 | 默认 |
|---|---|---|---|---|---|---|
| `tickers` | public | 否 | `instId` | `tickStreamTable` | P0 | 开 |
| `trades` | public | 否 | `instId` | `tradeStreamTable` | P0 | 开 |
| `books5` | public | 否 | `instId` | `bookStreamTable` | P1 | 关（`depth="books5"` 开） |
| `bbo-tbt` | public | 否 | `instId` | `bookStreamTable` | P1 | 关 |
| `books` | public | 否 | `instId` | `bookStreamTable` | P1 | 关（单独阶段） |
| `open-interest` | public | 否 | `instId` | `openinterestStreamTable` | P0 | 开 |
| `funding-rate` | public | 否 | `instId` | `fundingrateStreamTable` | P0 | 开 |
| `mark-price` | public | 否 | `instId` | `markpriceStreamTable` | P0 | 开 |
| `index-tickers` | public | 否 | `instId`（指数 ID） | `indextickStreamTable` | P0 | 开 |
| `price-limit` | public | 否 | `instId` | `pricelimitStreamTable` | P1 | 关 |
| `opt-summary` | public | 否 | `instFamily` | `optsummaryStreamTable` | P1 | 关（`options=True` 开） |
| `estimated-price` | public | 否 | `instType`/`instFamily` | `estimatedpriceStreamTable` | P1 | 关 |
| `option-trades` | public | 否 | `instFamily` | `optiontradeStreamTable` | P1 | 关 |
| `liquidation-orders` | public | 否 | `instType` | `liquidationorderStreamTable` | P0 | 开 |
| `status` | public | 否 | 无 | `statusStreamTable` | P0 | 开 |
| `instruments` | public | 否 | `instType` | `productStreamTable` | P0 | 开 |
| `candle1m` / `candle5m` | business | 否 | `instId` | `candleStreamTable`、`candleConfirmStreamTable` | P0 | 开 |
| `mark-price-candle*` | business | 否 | `instId` | `markpricecandleStreamTable` | P1 | 关 |
| `index-candle*` | business | 否 | `instId`（指数 ID） | `indexcandleStreamTable` | P1 | 关 |
| `trades-all` | business | 否 | `instId` | `tradeStreamTable` | P1 | 关 |
| `account` | private | 是 | 无 | `shareAccountTable` | P0 | 开（`private=True`） |
| `positions` | private | 是 | `instType` | `sharePositionTable` | P0 | 开 |
| `orders` | private | 是 | `instType` | `orderStreamTable`、`shareOrderTable` | P0 | 开 |
| `fills` | private | 是 | `instType` | `fillsStreamTable` | P1 | 关（VIP4+） |
| `balance_and_position` | private | 是 | 无 | `shareBalanceTable` | P1 | 开 |
| `liquidation-warning` | private | 是 | `instType` | `shareWarningTable` | P0 | 开 |
| `account-greeks` | private | 是 | `ccy` | `accountGreeksStreamTable` | P1 | 开（期权账户） |
| `orders-algo` | business-private | 是 | `instType` | `algoOrderStreamTable`、`shareAlgoOrderTable` | P1 | 关（`algo=True` 开） |
| `algo-advance` | business-private | 是 | `instType` | `algoOrderStreamTable`、`shareAlgoOrderTable` | P1 | 关 |

> `index-tickers` / `index-candle` 的 `instId` 不是 `BTC-USDT-SWAP`，而是 `BTC-USDT`。由 `to_index_id()` 统一转换。

### 3.2 默认频道组合

```python
DEFAULT_MARKET_CHANNELS = (
    "tickers",
    "trades",
    "funding-rate",
    "mark-price",
    "index-tickers",
    "open-interest",
    "liquidation-orders",
    "status",
    "instruments",
)
DEFAULT_CANDLE_CHANNELS = ("candle1m", "candle5m")
DEFAULT_PRIVATE_CHANNELS = (
    "account",
    "positions",
    "orders",
    "liquidation-warning",
    "balance_and_position",
    "account-greeks",
)
```

`Kernel` 新增可选参数：

```python
Kernel(
    pause=1,
    instIds={"BTC-USDT-SWAP"},
    flag="1",
    warmup=3,
    private=False,
    market=None,          # None -> DEFAULT_MARKET_CHANNELS
    depth=None,           # None / "books5" / "bbo-tbt" / "books"
    options=False,        # True -> opt-summary / option-trades / estimated-price
    algo=False,           # True -> orders-algo / algo-advance
    recreate_tables=False # 仅开发环境 True
)
```

旧调用 `Kernel(flag="1", pause=1, warmup=3, private=False)` 行为不变，但公共频道会比旧版多出资金费率、标记价格、指数、持仓量、产品、状态、爆仓单；如只想要最小流量，传 `market=("tickers", "candle1m", "candle5m")`。

---

## 4. 流数据表设计

### 4.1 表清单

`stream/schema.py` 中每张表用 `TableSpec` 描述；`script.py` 只负责拼接脚本。现有表列顺序保持不变，新增列一律追加在末尾。

| 表 | 类型 | 主键/时间列 | 说明 | 变更 |
|---|---|---|---|---|
| `tickStreamTable` | 流表 | - | ticker 实时总线 | 保持 |
| `shareTickTable` | latest | `instId` / `ts` | 最新 ticker 快照 | 保持 |
| `candleStreamTable` | 流表 | - | 所有 K 线（含未完成） | 保持 |
| `candleConfirmStreamTable` | 流表 | - | 仅 `confirm=1` 的已收盘 K 线 | 新增 |
| `shareCandleTable` | latest | `instId,channel` / `ts` | 最新 K 线快照 | 新增 |
| `candleIndicatorStreamTable` | 流表 | - | K 线指标输出 | 新增 |
| `shareCandleIndicatorTable` | latest | `instId,channel` / `ts` | 最新 K 线指标 | 新增 |
| `tickIndicatorStreamTable` | 流表 | - | tick 5 分钟窗口指标 | 新增 |
| `shareTickIndicatorTable` | latest | `instId` / `ts` | 最新 tick 指标 | 新增 |
| `tradeStreamTable` | 流表 | - | 公共逐笔成交（`trades` / `trades-all`） | 新增 |
| `bookStreamTable` | 流表 | - | 盘口长表（`side,level`） | 新增 |
| `openinterestStreamTable` | 流表 | - | 持仓量 | 保持 |
| `fundingrateStreamTable` | 流表 | - | 资金费率 | 追加字段 |
| `markpriceStreamTable` | 流表 | - | 标记价格 | 保持 |
| `markpricecandleStreamTable` | 流表 | - | 标记价格 K 线 | 修正列顺序 |
| `indextickStreamTable` | 流表 | - | 指数 ticker | 保持 |
| `indexcandleStreamTable` | 流表 | - | 指数 K 线 | 保持 |
| `pricelimitStreamTable` | 流表 | - | 限价 | 保持 |
| `optsummaryStreamTable` | 流表 | - | 期权摘要 | 追加 `volLv` |
| `estimatedpriceStreamTable` | 流表 | - | 交割/行权价 | 保持 |
| `optiontradeStreamTable` | 流表 | - | 期权成交 | 保持 |
| `liquidationorderStreamTable` | 流表 | - | 公共爆仓单 | 保持（多明细拆行） |
| `statusStreamTable` | 流表 | - | 系统维护状态 | 保持 |
| `productStreamTable` | 流表 | - | 产品规格 | 保持（真正落库） |
| `orderStreamTable` | 流表 | - | 订单事件历史 | 保持 |
| `shareOrderTable` | latest | `ordId` / `uTime` | 最新订单快照 | 新增 |
| `algoOrderStreamTable` | 流表 | - | 策略委托事件 | 新增 |
| `shareAlgoOrderTable` | latest | `algoId` / `uTime` | 最新策略委托快照 | 新增 |
| `fillsStreamTable` | 流表 | - | 成交明细（VIP4+） | 新增 |
| `shareAccountTable` | latest | `ccy` / `uTime` | 账户余额快照 | 保持 |
| `sharePositionTable` | latest | `instId,posSide` / `pTime` | 持仓快照 | 保持 |
| `shareBalanceTable` | latest | `ccy` / `uTime` | 余额快照 | 保持 |
| `shareWarningTable` | latest | `instId,posSide,posId` / `pTime` | 爆仓预警 | 保持 |
| `accountGreeksStreamTable` | 流表 | - | 账户 Greeks | 新增 |
| `shareAccountGreeksTable` | latest | `ccy` / `ts` | 最新 Greeks | 新增 |

### 4.2 TableSpec 实现

```python
# tradeflow/stream/schema.py
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from ..utils.format import align_frame


_PY_BY_DDB = {
    "DOUBLE": "float",
    "LONG": "int",
    "INT": "int",
    "TIMESTAMP": "ts",
    "BOOL": "bool",
    "SYMBOL": "str",
    "STRING": "str",
}


@dataclass(frozen=True)
class Column:
    name: str
    dtype: str
    py: str


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[Column, ...]
    key_columns: tuple[str, ...] = ()
    time_column: str = "ts"
    capacity: int = 200_000
    retention_minutes: int = 1440
    latest: bool = False
    description: str = ""

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def types(self) -> list[str]:
        return [c.dtype for c in self.columns]

    @property
    def type_map(self) -> dict[str, str]:
        return {c.name: c.py for c in self.columns}

    def normalize(self, frame: pd.DataFrame) -> pd.DataFrame:
        """按表结构对齐列、转换类型；所有 handler 都走这里，避免列顺序错位。"""
        return align_frame(frame, self.names, self.type_map)

    def exists_script(self) -> str:
        if self.latest:
            return f"defined(`{self.name}, SHARED)"
        return f"existsStreamTable(`{self.name})"

    def drop_script(self) -> str:
        if self.latest:
            return f"try {{ undef(`{self.name}, SHARED) }} catch(ex) {{}}"
        return f"try {{ dropStreamTable(`{self.name}) }} catch(ex) {{}}"

    def create_script(self) -> str:
        names = "`" + "`".join(self.names)
        types = ", ".join(self.types)
        var = f"__{self.name}"
        if self.latest:
            keys = "`" + "`".join(self.key_columns)
            return f"""
{var} = latestIndexedTable({keys}, `{self.time_column}, {self.capacity}:0, {names}, [{types}])
share {var} as {self.name}
"""
        return f"""
{var} = streamTable({self.capacity}:0, {names}, [{types}])
enableTableShareAndPersistence({var}, "{self.name}", false, true, preCache=0, cacheSize={self.capacity}, retentionMinutes={self.retention_minutes});
"""

    def ensure_script(self, recreate: bool = False) -> str:
        drop = self.drop_script() if recreate else ""
        return f"""
{drop}
if (!{self.exists_script()}) {{
{self.create_script()}
}}
"""


def cols(*specs: str) -> tuple[Column, ...]:
    """'name:TYPE[:py]' -> Column；未写 py 时按 DolphinDB 类型推断。"""
    result = []
    for spec in specs:
        parts = spec.split(":")
        name, dtype = parts[0], parts[1]
        py = parts[2] if len(parts) > 2 else _PY_BY_DDB[dtype]
        result.append(Column(name, dtype, py))
    return tuple(result)
```

> `pass` 是 DolphinDB 的空语句；如果担心版本差异，可改成 `1`。

### 4.3 关键表结构

```python
TICK = TableSpec(
    "tickStreamTable",
    cols(
        "instType:SYMBOL", "instId:SYMBOL", "last:DOUBLE", "lastSz:DOUBLE",
        "askPx:DOUBLE", "askSz:DOUBLE", "bidPx:DOUBLE", "bidSz:DOUBLE",
        "open24h:DOUBLE", "high24h:DOUBLE", "low24h:DOUBLE",
        "volCcy24h:DOUBLE", "vol24h:DOUBLE", "sodUtc0:DOUBLE",
        "sodUtc8:DOUBLE", "ts:TIMESTAMP",
    ),
    capacity=200_000,
    retention_minutes=360,
)

CANDLE = TableSpec(
    "candleStreamTable",
    cols(
        "ts:TIMESTAMP", "o:DOUBLE", "h:DOUBLE", "l:DOUBLE", "c:DOUBLE",
        "vol:DOUBLE", "volCcy:DOUBLE", "volCcyQuote:DOUBLE",
        "confirm:INT", "instId:SYMBOL", "channel:SYMBOL",
    ),
    capacity=500_000,
    retention_minutes=180,
)

TRADE = TableSpec(
    "tradeStreamTable",
    cols(
        "instId:SYMBOL", "tradeId:LONG", "px:DOUBLE", "sz:DOUBLE",
        "side:SYMBOL", "ts:TIMESTAMP", "count:INT", "source:SYMBOL",
    ),
    capacity=500_000,
    retention_minutes=180,
)

BOOK = TableSpec(
    "bookStreamTable",
    cols(
        "instId:SYMBOL", "ts:TIMESTAMP", "seqId:LONG", "prevSeqId:LONG",
        "checksum:INT", "side:SYMBOL", "level:INT", "px:DOUBLE",
        "sz:DOUBLE", "source:SYMBOL",
    ),
    capacity=1_000_000,
    retention_minutes=60,
)

FUNDING = TableSpec(
    "fundingrateStreamTable",
    cols(
        "fundingRate:DOUBLE", "fundingTime:LONG", "instId:SYMBOL",
        "instType:SYMBOL", "nextFundingRate:DOUBLE", "nextFundingTime:LONG",
        "premium:DOUBLE", "settState:SYMBOL", "settFundingRate:DOUBLE",
        "minFundingRate:DOUBLE", "maxFundingRate:DOUBLE",
        "interestRate:DOUBLE", "impactValue:DOUBLE", "method:SYMBOL",
        "formulaType:SYMBOL", "ts:TIMESTAMP",
    ),
    capacity=50_000,
    retention_minutes=1440,
)

CANDLE_INDICATOR = TableSpec(
    "candleIndicatorStreamTable",
    cols(
        "instId:SYMBOL", "channel:SYMBOL", "ts:TIMESTAMP",
        "ma5:DOUBLE", "ma10:DOUBLE", "ma20:DOUBLE",
        "ema12:DOUBLE", "ema26:DOUBLE", "macd:DOUBLE",
        "mstd20:DOUBLE", "volMa5:DOUBLE", "high20:DOUBLE", "low20:DOUBLE",
    ),
    capacity=200_000,
    retention_minutes=180,
)

TICK_INDICATOR = TableSpec(
    "tickIndicatorStreamTable",
    cols(
        "ts:TIMESTAMP", "instId:SYMBOL", "count5m:LONG",
        "open5m:DOUBLE", "high5m:DOUBLE", "low5m:DOUBLE",
        "close5m:DOUBLE", "avgPx:DOUBLE", "vol5m:DOUBLE",
    ),
    capacity=200_000,
    retention_minutes=180,
)
```

> `orderStreamTable` / `sharePositionTable` / `shareWarningTable` 列很多，实施时把 `stream/script.py` 现有 `colName` / `colType` 原样搬进 `cols(...)`，不要重排列顺序。新增的 `shareOrderTable` 直接复用 `orderStreamTable` 的列定义，只把 `latest=True`、`key_columns=("ordId",)`、`time_column="uTime"`。

### 4.4 流表 vs 历史表

- 流表开启 `enableTableShareAndPersistence`，但 `preCache=0`：只保证进程重启后能继续发布，不负责长期历史。
- 历史数据由 `stream/archive.py`（P1）用 `subscribeTable` 写入 `dfs://market`：
  - `tick`：按 `date(ts)` + `hash(instId, 30)` 分区；
  - `trade`：按 `date(ts)` + `hash(instId, 30)` 分区；
  - `candle`：按 `date(ts)` + `hash(instId, 30)` 分区。
- 归档订阅参数：`batchSize=10000`、`throttle=5`、`persistOffset=true`，避免流表 `retentionMinutes` 到期后历史丢失。

---

## 5. 流计算引擎设计

### 5.1 引擎矩阵

| 引擎/订阅 | 类型 | 输入 | 输出 | 作用 |
|---|---|---|---|---|
| `lastTickAction` | `subscribeTable` + `tableInsert` | `tickStreamTable` | `shareTickTable` | 最新 ticker |
| `lastCandleAction` | `subscribeTable` + `tableInsert` | `candleStreamTable` | `shareCandleTable` | 最新 K 线（含未完成） |
| `lastOrderAction` | `subscribeTable` + `tableInsert` | `orderStreamTable` | `shareOrderTable` | 最新订单状态 |
| `lastAlgoOrderAction` | `subscribeTable` + `tableInsert` | `algoOrderStreamTable` | `shareAlgoOrderTable` | 最新策略单状态 |
| `lastAccountGreeksAction` | `subscribeTable` + `tableInsert` | `accountGreeksStreamTable` | `shareAccountGreeksTable` | 最新 Greeks |
| `lastCandleIndicatorAction` | `subscribeTable` + `tableInsert` | `candleIndicatorStreamTable` | `shareCandleIndicatorTable` | 最新 K 线指标快照 |
| `lastTickIndicatorAction` | `subscribeTable` + `tableInsert` | `tickIndicatorStreamTable` | `shareTickIndicatorTable` | 最新 tick 活跃度快照 |
| `candleIndicatorEngine` | `createReactiveStateEngine` | `candleConfirmStreamTable` | `candleIndicatorStreamTable` | K 线均线/MACD/波动 |
| `tickIndicatorEngine` | `createTimeSeriesEngine` | `tickStreamTable` | `tickIndicatorStreamTable` | 5 分钟活跃度/价格区间 |

> 生命周期约束（实测验证）：**重建流表之前必须先摘除引擎订阅**——响应式状态
> 引擎持有其输出表引用，活跃订阅会阻塞 `dropStreamTable`。`Kernel.prepare()`
> 在 `recreate_tables=True` 时先对 `ENGINE_SPECS` 全量 `unsubscribeTable` +
> `dropStreamEngine`，再建表，最后重建引擎。

### 5.2 K 线指标引擎

只喂 `confirm=1` 的 K 线，避免未完成 bar 污染状态。Python 侧在 `parse_candle()` 中同时产出两个 `Batch`：

- `candleStreamTable`：全部 bar；
- `candleConfirmStreamTable`：仅 `confirm == 1`。

DolphinDB 脚本：

```dolphindb
try { unsubscribeTable(tableName=`candleConfirmStreamTable, actionName="candleIndicatorAction") } catch(ex) {}
try { dropStreamEngine(`candleIndicatorEngine) } catch(ex) {}

candleIndicatorEngine = createReactiveStateEngine(
    name="candleIndicatorEngine",
    metrics=<[
        ts,
        mavg(c, 5, 1) as ma5,
        mavg(c, 10, 1) as ma10,
        mavg(c, 20, 1) as ma20,
        ema(c, 12) as ema12,
        ema(c, 26) as ema26,
        ema(c, 12) - ema(c, 26) as macd,
        mstd(c, 20, 1) as mstd20,
        mavg(vol, 5, 1) as volMa5,
        mmax(h, 20, 1) as high20,
        mmin(l, 20, 1) as low20
    ]>,
    dummyTable=candleConfirmStreamTable,
    outputTable=candleIndicatorStreamTable,
    keyColumn=`instId`channel
)

subscribeTable(
    tableName=`candleConfirmStreamTable,
    actionName="candleIndicatorAction",
    offset=0,
    handler=tableInsert{candleIndicatorEngine},
    msgAsTable=true
)
```

输出表列顺序必须是“分组列 → 时间列 → 指标列”，即：

```text
instId, channel, ts, ma5, ma10, ma20, ema12, ema26, macd, mstd20, volMa5, high20, low20
```

### 5.3 tick 活跃度引擎

```dolphindb
try { unsubscribeTable(tableName=`tickStreamTable, actionName="tickIndicatorAction") } catch(ex) {}
try { dropStreamEngine(`tickIndicatorEngine) } catch(ex) {}

tickIndicatorEngine = createTimeSeriesEngine(
    name="tickIndicatorEngine",
    windowSize=300000,
    step=60000,
    metrics=<[
        count(*) as count5m,
        first(last) as open5m,
        max(last) as high5m,
        min(last) as low5m,
        last(last) as close5m,
        avg(last) as avgPx,
        sum(lastSz) as vol5m
    ]>,
    dummyTable=tickStreamTable,
    outputTable=tickIndicatorStreamTable,
    timeColumn=`ts,
    useSystemTime=false,
    keyColumn=`instId,
    useWindowStartTime=false,
    fill="none"
)

subscribeTable(
    tableName=`tickStreamTable,
    actionName="tickIndicatorAction",
    offset=0,
    handler=tableInsert{tickIndicatorEngine},
    msgAsTable=true
)
```

- `windowSize=300000` 表示 5 分钟（`ts` 为 TIMESTAMP，毫秒精度）。
- `step=60000` 表示每 1 分钟滚动输出一次。
- `useWindowStartTime=false` 表示输出窗口结束时间，便于 `order by ts desc`。
- `count5m` 对应 `Kernel.getTradeInstid()` 的流动性排序。

### 5.4 引擎生命周期

- 每次 `Kernel.prepare()` 先 `unsubscribeTable`，再 `dropStreamEngine`，再重建；避免重复订阅和旧状态。
- 引擎输出表必须提前创建，且列顺序/类型与 metrics 完全一致；`schema.py` 是唯一来源。
- 生产环境可给 `candleIndicatorEngine` / `tickIndicatorEngine` 配 `snapshotDir`、`snapshotIntervalInMsgCount`，进程重启后恢复窗口状态；本阶段先不做，写入 README 的“可选项”。

---

## 6. 订阅模块设计

### 6.1 数据流

```text
OKX WS
  │
  ├─ BaseSubscribe._consume()           # 只做收包、心跳、重连
  │     └─ handlers.dispatch(packet)    # 纯函数，异常不外溢
  │           └─ list[Batch(table, df)]
  │
  └─ StreamWriter.put(table, df)        # 非阻塞入队
        └─ writer 线程：按表 concat -> TableSpec.normalize -> tableInsert
              └─ DolphinDB stream table
                    ├─ latestIndexedTable（快照）
                    ├─ ReactiveStateEngine（K线指标）
                    └─ TimeSeriesEngine（tick 活跃度）
```

### 6.2 `Batch` 与 `dispatch`

```python
# tradeflow/subscribe/handlers/__init__.py
from dataclasses import dataclass

import pandas as pd

from ...utils.log import get_logger

logger = get_logger()


@dataclass
class Batch:
    table: str
    frame: pd.DataFrame


def channel_key(channel: str) -> str:
    if channel.startswith("mark-price-candle"):
        return "mark-price-candle"
    if channel.startswith("index-candle"):
        return "index-candle"
    if channel.startswith("candle"):
        return "candle"
    return channel


def dispatch(packet: dict) -> list[Batch]:
    arg = packet.get("arg") or {}
    channel = arg.get("channel", "")
    if not channel:
        return []
    handler = HANDLERS.get(channel_key(channel))
    if handler is None:
        logger.warning(f"未实现的频道，已忽略: {channel}")
        return []
    try:
        return [b for b in handler(packet) if b.frame is not None and not b.frame.empty]
    except Exception as exc:
        logger.exception(f"解析频道 {channel} 失败，已丢弃本条: {exc}")
        return []
```

### 6.3 公共行情解析要点

- `tickers`：DataFrame 直出，`ts` 由 `TableSpec.normalize` 统一转 TIMESTAMP。
- `candle*`：OKX 返回 `[ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]`；同时写 `candleStreamTable` 和 `candleConfirmStreamTable`。
- `trades` / `trades-all`：统一写 `tradeStreamTable`，用 `source` 区分；缺失 `count` 补 1。
- `books5` / `bbo-tbt`：把 `asks`/`bids` 展开成长表，`side=ask/bid`、`level=1..N`。
- `liquidation-orders`：遍历 `data[*].details[*]`，每笔明细一行，不能只取 `details[0]`。
- `funding-rate`：全部字段保留；`nextFundingRate=""` 交给 `normalize` 转 0，不再手写 `float()`。
- `mark-price-candle` / `index-candle`：列顺序固定为 `[channel, instId, ts, o,h,l,c,confirm]`，不要写反。
- `opt-summary`：追加 `volLv`，列数不足由 `normalize` 补 0。
- `instruments`：真正写 `productStreamTable`，`listTime/expTime` 转 TIMESTAMP。
- `status` / `price-limit` / `estimated-price` / `option-trades`：按文档字段直出，统一走 `normalize`。

### 6.4 私有频道解析要点

- `account`：只保留 `ACCOUNT_COLUMNS`，新字段丢弃；数值列空串转 0。
- `positions`：保留 `posSide`，`sharePositionTable` 主键为 `instId,posSide`；双向持仓不再互相覆盖。
- `orders`：写 `orderStreamTable` 历史 + `shareOrderTable` 最新快照；`ordId` 统一 int64。
- `fills`：VIP4+ 才可用；收到 `64003` 时自动关闭该频道并告警，不影响 `orders`。
- `balance_and_position`：写 `shareBalanceTable`。
- `liquidation-warning`：写 `shareWarningTable`。
- `account-greeks`：写 `accountGreeksStreamTable`，并更新 `shareAccountGreeksTable`。
- `orders-algo` / `algo-advance`：共用 `algoOrderStreamTable`，`ordIdList`/`subAlgoIdList` 用逗号拼接为 STRING，完整原始 JSON 存 `raw`。

### 6.5 连接管理

```python
# tradeflow/subscribe/base.py（核心骨架）
class Subscribe:
    endpoint = "public"
    login_required = False

    def __init__(self, kernel, channels, active=True, interval=5, timeout=30,
                 writer=None, reconciler=None, url=None):
        self.kernel = kernel
        self.active = active
        self.interval = interval
        self.timeout = timeout
        self.writer = writer
        self.reconciler = reconciler
        self.url = url or self.default_url()
        self._desired = {self._key(arg): arg for arg in channels}
        self._ws = None
        self._connected = False
        self._last_message_at = 0.0
        self._reconnect_count = 0
        self._error_count = 0
        self._request_count = 0
        self._send_lock = asyncio.Lock()
        self._heartbeat_task = None

    async def run(self):
        backoff = self.interval
        while self.active:
            try:
                await self._connect()
                backoff = self.interval
                await self._consume()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._error_count += 1
                self._reconnect_count += 1
                self.onError("连接中断, 尝试重连", exc)
                await self._close()
                if not self.active:
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _connect(self):
        self._ws = await websockets.connect(
            self.url,
            ping_interval=None,
            close_timeout=5,
            max_queue=4096,
        )
        if self.login_required:
            await self._send_text(self.loginParam)
            raw = await asyncio.wait_for(self._ws.recv(), timeout=10)
            packet = json.loads(raw)
            if packet.get("event") != "login" or packet.get("code") != "0":
                raise RuntimeError(f"OKX 登录失败: {packet}")
        await self._send_json({"op": "subscribe", "args": list(self._desired.values())})
        self._connected = True
        self._last_message_at = time.time()
        self._heartbeat_task = asyncio.create_task(self._heartbeat())
        if self.reconciler is not None:
            await asyncio.to_thread(self.reconciler.after_reconnect, self.endpoint)

    async def _consume(self):
        while self.active and self._ws is not None:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=self.timeout)
            self._last_message_at = time.time()
            if raw == "pong":
                continue
            packet = json.loads(raw)
            if "event" in packet:
                self._handle_event(packet)
                continue
            for batch in dispatch(packet):
                if self.writer is not None:
                    self.writer.put(batch.table, batch.frame)

    async def _heartbeat(self):
        while self.active and self._ws is not None:
            await asyncio.sleep(20)
            if time.time() - self._last_message_at >= 20:
                try:
                    await self._send_text("ping")
                except Exception:
                    return

    def _handle_event(self, packet):
        event = packet.get("event")
        if event == "error":
            code = str(packet.get("code", ""))
            self._error_count += 1
            logger.error(f"<{self.__class__.__name__}> 频道错误: {packet}")
            arg = packet.get("arg") or {}
            if arg.get("channel"):
                self._desired.pop(self._key(arg), None)
            if code in {"60009", "60005", "60006", "60007", "60010", "60011"}:
                raise RuntimeError(f"OKX 连接级错误 {code}: {packet.get('msg')}")
            return
        if event == "notice":
            logger.warning(f"<{self.__class__.__name__}> notice: {packet}")
            return
        if event in {"channel-conn-count", "channel-conn-count-error"}:
            logger.warning(f"<{self.__class__.__name__}> 连接数通知: {packet}")
            return
        if event in {"subscribe", "unsubscribe", "login"}:
            logger.info(f"<{self.__class__.__name__}> {event}: {packet.get('arg', packet.get('msg', ''))}")
            return
        logger.warning(f"<{self.__class__.__name__}> 未知事件: {packet}")
```

`subscribe()` / `unsubscribe()`：

```python
async def subscribe(self, args):
    fresh = []
    for arg in args:
        key = self._key(arg)
        if key not in self._desired:
            self._desired[key] = arg
            fresh.append(arg)
    if fresh and self._ws is not None and self._connected:
        await self._send_json({"op": "subscribe", "args": fresh})

async def unsubscribe(self, args):
    removed = []
    for arg in args:
        key = self._key(arg)
        if self._desired.pop(key, None) is not None:
            removed.append(arg)
    if removed and self._ws is not None and self._connected:
        await self._send_json({"op": "unsubscribe", "args": removed})
```

### 6.6 异步批量落库

`StreamWriter` 使用独立线程 + 独立 DolphinDB session，避免阻塞 asyncio 事件循环，也避免和主线程共用 `Dolphin.session`。

```python
class StreamWriter:
    def __init__(self, dolphin, batch_size=200, flush_interval=0.2, queue_size=200_000):
        self._dolphin = dolphin
        self._session = dolphin.newSession()
        self._queue = queue.Queue(maxsize=queue_size)
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._stop = threading.Event()
        self._thread = Thread(target=self._loop, daemon=True)
        self._written = 0
        self._dropped = 0
        self._errors = 0
        self._thread.start()

    def put(self, table, frame):
        if frame is None or frame.empty:
            return
        try:
            self._queue.put_nowait((table, frame))
        except queue.Full:
            self._dropped += len(frame)
            if self._dropped % 1000 < len(frame):
                logger.error(f"写队列已满，累计丢弃 {self._dropped} 行")

    def _loop(self):
        while not self._stop.is_set() or not self._queue.empty():
            batch = []
            rows = 0
            try:
                item = self._queue.get(timeout=self._flush_interval)
                batch.append(item)
                rows += len(item[1])
            except queue.Empty:
                continue
            deadline = time.monotonic() + self._flush_interval
            while rows < self._batch_size:
                timeout = max(0.0, deadline - time.monotonic())
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    break
                batch.append(item)
                rows += len(item[1])
            self._flush(batch)
```

`_flush` 按表合并，调用 `TableSpec.normalize`，失败时按 100 行拆分重试，避免一条脏数据毁掉整批。

### 6.7 断线对账与补齐

`reconcile.py`：

```python
import pandas as pd

from ..stream.schema import STREAM_TABLES


class Reconciler:
    def __init__(self, kernel):
        self.kernel = kernel

    def after_reconnect(self, endpoint):
        if endpoint in ("private", "business-private"):
            self.sync_orders()
            self.sync_positions()
            self.sync_account()
        if endpoint == "business":
            self.backfill_candles()

    def sync_orders(self):
        packet = self.kernel.api.trade.get_order_list(instType="ANY")
        if packet.get("code") != "0":
            return
        rows = packet.get("data") or []
        if rows:
            frame = STREAM_TABLES["shareOrderTable"].normalize(pd.DataFrame(rows))
            self.kernel.dolphin.data2MemoryTable("shareOrderTable", frame)

    def sync_positions(self):
        packet = self.kernel.api.account.get_positions(instType="ANY")
        if packet.get("code") != "0":
            return
        rows = packet.get("data") or []
        if rows:
            frame = STREAM_TABLES["sharePositionTable"].normalize(pd.DataFrame(rows))
            self.kernel.dolphin.data2MemoryTable("sharePositionTable", frame)

    def sync_account(self):
        packet = self.kernel.api.account.get_account_balance()
        if packet.get("code") != "0":
            return
        details = (packet.get("data") or [{}])[0].get("details") or []
        if details:
            frame = STREAM_TABLES["shareAccountTable"].normalize(pd.DataFrame(details))
            self.kernel.dolphin.data2MemoryTable("shareAccountTable", frame)

    def backfill_candles(self):
        for instId in self.kernel.instIds:
            last = self.kernel.dolphin.runScript(
                f"exec max(ts) from candleStreamTable where instId='{instId}'"
            )
            if last is None:
                continue
            packet = self.kernel.api.market.get_history_candlesticks(
                instId, after="", before="", bar="1m", limit="300"
            )
            rows = packet.get("data") or []
            if rows:
                df = pd.DataFrame(rows, columns=[
                    "ts", "o", "h", "l", "c", "vol", "volCcy",
                    "volCcyQuote", "confirm",
                ])
                df["instId"] = instId
                df["channel"] = "candle1m"
                frame = STREAM_TABLES["candleStreamTable"].normalize(df)
                self.kernel.dolphin.data2MemoryTable("candleStreamTable", frame)
```

> 对账只做“快照覆盖”，不重放历史订单事件；订单历史以 `orderStreamTable` + REST `get_orders_history` 为准。

### 6.8 健康指标

`SubscribeManager.health()` 返回：

```python
{
    "public": {
        "connected": True,
        "last_message_at": 1760000000.0,
        "reconnect_count": 0,
        "error_count": 0,
        "channels": ["tickers:BTC-USDT-SWAP", ...],
    },
    "writer": {"queue": 0, "written": 12345, "dropped": 0, "errors": 0},
}
```

`Kernel.health()` 直接透传；建议策略每 60 秒打一条健康日志，或接入邮件告警。

---

## 7. Kernel 集成

### 7.1 `prepare()`

```python
def prepare(self):
    enabled = self.enabled_channels()
    tables = set(CORE_TABLES)
    for channel in enabled:
        spec = get_channel_spec(channel)
        if spec is not None:
            tables.update(spec.tables)
    self.dolphin.prepare_streams(tables, recreate=self.recreate_tables)
    self.dolphin.prepare_engines(ENGINE_NAMES)
```

`CORE_TABLES` 至少包含：

```python
CORE_TABLES = {
    "tickStreamTable", "shareTickTable",
    "candleStreamTable", "candleConfirmStreamTable", "shareCandleTable",
    "orderStreamTable", "shareOrderTable",
    "shareAccountTable", "sharePositionTable", "shareWarningTable",
}
```

### 7.2 频道参数生成

```python
DEFAULT_MARKET_CHANNELS = (
    "tickers", "trades", "funding-rate", "mark-price",
    "index-tickers", "open-interest", "liquidation-orders",
    "status", "instruments",
)
DEFAULT_CANDLE_CHANNELS = ("candle1m", "candle5m")
DEFAULT_PRIVATE_CHANNELS = (
    "account", "positions", "orders", "liquidation-warning",
    "balance_and_position", "account-greeks",
)


def to_index_id(inst_id: str) -> str:
    """BTC-USDT-SWAP -> BTC-USDT；非合约原样返回。"""
    parts = inst_id.split("-")
    return "-".join(parts[:2]) if len(parts) >= 2 else inst_id


class Kernel:
    def enabled_channels(self) -> set[str]:
        names = set(self.market or DEFAULT_MARKET_CHANNELS)
        names.update(DEFAULT_CANDLE_CHANNELS)
        if self.depth:
            names.add(self.depth)
        if self.options:
            names.update({"opt-summary", "option-trades", "estimated-price"})
        if self.private_enabled:
            names.update(DEFAULT_PRIVATE_CHANNELS)
        if self.algo_enabled:
            names.update({"orders-algo", "algo-advance"})
        return names

    @property
    def publicChannels(self):
        channels = []
        for name in self.market or DEFAULT_MARKET_CHANNELS:
            if name == "status":
                channels.append({"channel": name})
            elif name == "instruments":
                channels.append({"channel": name, "instType": "SWAP"})
            elif name == "liquidation-orders":
                channels.append({"channel": name, "instType": "SWAP"})
            elif name == "index-tickers":
                channels.extend(
                    {"channel": name, "instId": to_index_id(instId)}
                    for instId in self.instIds
                )
            else:
                channels.extend(
                    {"channel": name, "instId": instId}
                    for instId in self.instIds
                )
        if self.depth:
            channels.extend(
                {"channel": self.depth, "instId": instId}
                for instId in self.instIds
            )
        if self.options:
            families = {
                "-".join(instId.split("-")[:2])
                for instId in self.instIds
                if instId.count("-") >= 2
            }
            channels.extend(
                {"channel": "opt-summary", "instFamily": family}
                for family in families
            )
            channels.extend(
                {"channel": "option-trades", "instFamily": family}
                for family in families
            )
        return channels

    @property
    def candleChannels(self):
        return [
            {"channel": channel, "instId": instId}
            for instId in self.instIds
            for channel in DEFAULT_CANDLE_CHANNELS
        ]

    @property
    def privateChannels(self):
        channels = []
        for name in DEFAULT_PRIVATE_CHANNELS:
            if name == "account":
                channels.append({"channel": name})
            elif name == "balance_and_position":
                channels.append({"channel": name})
            elif name == "account-greeks":
                channels.append({"channel": name})
            else:
                channels.append({"channel": name, "instType": "ANY"})
        return channels

    @property
    def algoChannels(self):
        return [
            {"channel": name, "instType": "ANY"}
            for name in ("orders-algo", "algo-advance")
        ]
```

### 7.3 `subscribe()` / `stop()`

```python
def subscribe(self):
    if not self._thread.is_alive():
        self._thread.start()
    self.manager = SubscribeManager(self)
    self.manager.start()
    self.public = self.manager.public
    self.candle = self.manager.business
    self.private = self.manager.private
    self.algo = self.manager.algo

def stop(self):
    self._active = False
    if self.manager is not None:
        self.manager.stop()
    for task in self._tasks:
        task.cancel()
    if self._loop.is_running():
        self._loop.call_soon_threadsafe(self._loop.stop)
    if self._thread.is_alive():
        self._thread.join(timeout=5)
```

### 7.4 查询接口

```python
def getLastPositions(self):
    """net_mode 返回 {instId: row}；双向持仓返回 {f"{instId}:{posSide}": row}。"""
    df = self.dolphin.runScript("select * from sharePositionTable")
    if df is None or df.empty:
        return {}
    long_short = "posSide" in df.columns and (df["posSide"].fillna("") != "").any()
    if long_short:
        return {
            f"{row.instId}:{row.posSide}": row._asdict()
            for row in df.itertuples(index=False)
        }
    return df.set_index("instId").T.to_dict()

def getLastOrders(self):
    return self.dolphin.runScript("select * from shareOrderTable")

def getLastCandle(self, channel="candle1m"):
    return self.dolphin.runScript(
        f"select * from shareCandleTable where channel='{channel}'"
    )
```

`RiskManage.run()` 改用 `getPositions()`（返回 `list[dict]`），并加 `_closing` 集合，避免同一仓位在 1 秒内重复发平仓单：

```python
class RiskManage:
    def __init__(self, ...):
        # ... 原有字段 ...
        self._closing = set()

    def run(self):
        for position in self.kernel.getPositions():
            instId = position["instId"]
            posSide = position.get("posSide") or "net"
            key = (instId, posSide)
            pos = float(position.get("pos") or 0)
            uplRatio = float(position.get("uplRatio") or 0)
            if pos == 0 or key in self._closing:
                continue
            if uplRatio < -self.maxLoss or uplRatio > self.maxProfit:
                self._closing.add(key)
                try:
                    packet = self.kernel.api.trade.close_positions(
                        instId, position["mgnMode"], posSide, ccy=position.get("ccy", "")
                    )
                    self.log(f"close {key} -> {packet.get('code')}")
                finally:
                    self._closing.discard(key)
```

`Kernel.getPositions()` 新增：

```python
def getPositions(self):
    df = self.dolphin.runScript("select * from sharePositionTable")
    if df is None or df.empty:
        return []
    return df.to_dict("records")
```

### 7.5 `Strategy` 查询

```python
def getCandleIndicator(self, limit=60, channel="candle1m"):
    script = f"""
    with temp as (
        select * from candleIndicatorStreamTable
        where channel = '{channel}'
        context by instId csort ts desc limit {limit}
    )
    select * from temp order by ts
    """
    data = self.kernel.dolphin.runScript(script)
    return Indicator(data.drop_duplicates(subset=["ts", "instId"], keep="last"), limit)

def getLastCandleIndicators(self, channel="candle1m"):
    data = self.kernel.dolphin.runScript(
        f"select * from shareCandleIndicatorTable where channel='{channel}'"
    )
    return data.set_index("instId").T.to_dict()
```

---

## 8. 迁移与兼容

1. **导入兼容**：`subscribe/__init__.py` 保留旧类名；`Kernel` 继续暴露 `self.public`、`self.candle`、`self.private`。
2. **表结构兼容**：旧表列顺序不变，新列追加末尾；`prepare(recreate=False)` 默认不删表，启动时比对 `schema`，列不一致直接报 `SchemaMismatchError`，提示设置 `TRADEFLOW_RECREATE_TABLES=1`。
3. **时间语义**：`ts2Timestamp` 统一转 UTC-naive `TIMESTAMP`；DolphinDB 中不再写本地时区时间。旧数据如仍是本地时间，需一次性重建或手工加时区偏移。
4. **配置兼容**：保留 `TIMEOUT`，新增 `WS_RECV_TIMEOUT`（默认 30）、`WS_HEARTBEAT_INTERVAL`（默认 20）、`WS_RECONNECT_MAX`（默认 30）、`WS_QUEUE_SIZE`（默认 200000）、`WS_BATCH_SIZE`（默认 200）、`WS_FLUSH_INTERVAL`（默认 0.2）。
5. **版本**：升级到 `3.4.0`；README 增加频道矩阵、表清单、迁移步骤和健康检查示例。

---

## 9. 测试策略

### 9.1 单元测试（无需网络、无需 DolphinDB）

- `tests/test_format.py`：时间、空串、列对齐、bool/int/float 转换。
- `tests/test_schema.py`：所有频道引用的表都存在；所有表列顺序与 DolphinDB 脚本一致；`normalize` 补列/转类型。
- `tests/test_handlers_market.py`：ticker、candle、trades、books5、funding、mark-price、index-tickers、liquidation、instruments、status、opt-summary。
- `tests/test_handlers_private.py`：account、positions、orders、fills、balance、warning、greeks、algo。
- `tests/test_writer.py`：批量合并、按表分组、normalize 调用、队列满丢弃、异常重试。
- `tests/test_subscribe_base.py`：事件处理、频道错误隔离、动态订阅集合、health。
- `tests/test_kernel_channels.py`：默认频道、`depth`/`options`/`algo` 开关、`publicChannels` 参数生成。
- `tests/test_reconcile.py`：对账 API 调用、K 线补齐过滤。

### 9.2 DolphinDB 集成测试

`tests/integration/test_stream_schema.py`（`TRADEFLOW_TEST_DDB=1`）：

1. 连接 `localhost:8848`；
2. `prepare_streams(all_tables, recreate=True)`；
3. 用 `replay` 或 `tableInsert` 写入 20 条 tick、10 条 candle；
4. 等待 2 秒；
5. 断言 `shareTickTable`、`shareCandleTable`、`candleIndicatorStreamTable`、`tickIndicatorStreamTable` 有数据；
6. 断言 `getStreamingStat().subWorkers` 中存在对应 action。

### 9.3 实盘冒烟测试

`tests/integration/test_live_okx.py`（`TRADEFLOW_TEST_LIVE=1`）：

1. `Kernel(flag="1", warmup=5, private=False)`；
2. 运行 30 秒；
3. 断言 `tickStreamTable`、`tradeStreamTable`、`candleStreamTable` 行数 > 0；
4. 断言 `kernel.health()["public"]["error_count"] == 0`；
5. 手动断开网络 5 秒，断言 `reconnect_count >= 1` 且恢复后继续写入。

### 9.4 回归命令

```bash
pytest -q
# test_stream_schema 会重建流表，必须指向独立测试实例
TRADEFLOW_TEST_DDB=1 TRADEFLOW_TEST_DDB_ALLOW_DROP=1 pytest tests/integration/test_stream_schema.py -q
TRADEFLOW_TEST_LIVE=1 pytest tests/integration/test_live_okx.py -q -s
python -m strategy.example
```

---

## 10. 实施任务

### Task 1: 数据格式化工具

**Files:**
- Create: `tradeflow/utils/format.py`
- Create: `tests/test_format.py`
- Modify: `tradeflow/utils/__init__.py`
- Modify: `tradeflow/utils/method.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_format.py
import pandas as pd

from tradeflow.utils.format import align_frame, as_float, as_int, ts2Timestamp


def test_ts2timestamp_utc():
    assert ts2Timestamp("1704067200000") == pd.Timestamp("2024-01-01 00:00:00")


def test_empty_numeric():
    assert as_float("") == 0.0
    assert as_float("unsupported") == 0.0
    assert as_int("12.9") == 12


def test_align_frame_orders_and_casts():
    df = pd.DataFrame({"b": ["2"], "a": [""]})
    out = align_frame(df, ["a", "b"], {"a": "float", "b": "int"})
    assert list(out.columns) == ["a", "b"]
    assert out.loc[0, "a"] == 0.0
    assert out.loc[0, "b"] == 2
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_format.py -q`
Expected: `ModuleNotFoundError: No module named 'tradeflow.utils.format'`

- [ ] **Step 3: 实现 `format.py`**

```python
# tradeflow/utils/format.py
"""订阅数据格式化纯函数：不依赖 Kernel、DolphinDB、网络。"""
from __future__ import annotations

import json
from typing import Any, Iterable

import pandas as pd

_EMPTY = ("", "unsupported", "off", None)


def as_float(value: Any, default: float = 0.0) -> float:
    if value in _EMPTY:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    if value in _EMPTY:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def ts2Timestamp(value: Any) -> pd.Timestamp:
    """OKX 毫秒时间戳 -> UTC-naive pandas Timestamp；空值返回 NaT。"""
    if value in _EMPTY or value == 0:
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            return value.tz_convert("UTC").tz_localize(None)
        return value
    if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
        return pd.to_datetime(value, utc=True).tz_localize(None)
    return pd.to_datetime(int(float(value)), unit="ms", utc=True).tz_localize(None)


def align_frame(
    frame: pd.DataFrame,
    columns: Iterable[str],
    type_map: dict[str, str],
) -> pd.DataFrame:
    """按目标列顺序重排，并按 float/int/ts/bool/str 转换；缺失列补默认值。"""
    out = frame.reindex(columns=list(columns))
    for column, kind in type_map.items():
        if column not in out.columns:
            continue
        if kind == "float":
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0).astype("float64")
        elif kind == "int":
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0).astype("int64")
        elif kind == "ts":
            out[column] = out[column].map(ts2Timestamp)
        elif kind == "bool":
            out[column] = out[column].fillna(False).astype(bool)
        else:
            out[column] = out[column].fillna("").astype(str)
    return out


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
```

`utils/__init__.py` 统一导出：

```python
from .format import align_frame, as_float, as_int, json_dumps, ts2Timestamp

__all__ = ["align_frame", "as_float", "as_int", "json_dumps", "ts2Timestamp"]
```

`method.py` 删除重复的 `ts2Timestamp` / `dfApply` 实现，改为：

```python
from .format import ts2Timestamp
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_format.py -q`
Expected: `3 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/utils/format.py tradeflow/utils/__init__.py tradeflow/utils/method.py tests/test_format.py
git commit -m "feat: 统一订阅数据格式化工具"
```

---

### Task 2: 表结构注册表

**Files:**
- Create: `tradeflow/stream/schema.py`
- Create: `tests/test_schema.py`
- Modify: `tradeflow/stream/script.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_schema.py
import pandas as pd

from tradeflow.stream.schema import STREAM_TABLES, TICK, FUNDING


def test_tick_columns_order():
    assert TICK.names[:3] == ["instType", "instId", "last"]
    assert TICK.types[-1] == "TIMESTAMP"


def test_funding_empty_next_rate():
    df = pd.DataFrame([{
        "instId": "BTC-USD-SWAP",
        "instType": "SWAP",
        "fundingRate": "0.0001",
        "fundingTime": "1704067200000",
        "nextFundingRate": "",
        "nextFundingTime": "1704096000000",
        "ts": "1704067200000",
    }])
    out = FUNDING.normalize(df)
    assert out.loc[0, "nextFundingRate"] == 0.0
    assert out.loc[0, "fundingRate"] == 0.0001
    assert str(out["ts"].dtype).startswith("datetime64")


def test_create_script_is_idempotent():
    script = TICK.ensure_script(recreate=False)
    assert "existsStreamTable(`tickStreamTable)" in script
    assert "enableTableShareAndPersistence" in script
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_schema.py -q`
Expected: `ModuleNotFoundError: No module named 'tradeflow.stream.schema'`

- [ ] **Step 3: 实现 `TableSpec` 与全部表**

把第 4.2 节的 `TableSpec`/`Column`/`cols` 原样写入 `schema.py`，然后：

1. 把 `stream/script.py` 中 `createTickStreamTableScript`、`createCandleStreamTableScript`、`createOrderStreamTableScript`、`createSharePositionTableScript`、`createShareWarningTableScript` 等所有 `colName`/`colType` 搬进 `STREAM_TABLES`，列顺序不变；
2. 新增 `candleConfirmStreamTable`、`shareCandleTable`、`candleIndicatorStreamTable`、`shareCandleIndicatorTable`、`tickIndicatorStreamTable`、`shareTickIndicatorTable`、`tradeStreamTable`、`bookStreamTable`、`shareOrderTable`、`algoOrderStreamTable`、`shareAlgoOrderTable`、`fillsStreamTable`、`accountGreeksStreamTable`、`shareAccountGreeksTable`；
3. `script.py` 保留旧常量名，内容改为从 `STREAM_TABLES` 生成：

```python
from .schema import STREAM_TABLES

createTickStreamTableScript = STREAM_TABLES["tickStreamTable"].create_script()
createCandleStreamTableScript = STREAM_TABLES["candleStreamTable"].create_script()
# ... 其余表同理
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_schema.py -q`
Expected: `3 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/stream/schema.py tradeflow/stream/script.py tests/test_schema.py
git commit -m "feat: 建立 DolphinDB 流表结构注册表"
```

---

### Task 3: DolphinDB 幂等建表与独立 session

**Files:**
- Modify: `tradeflow/stream/dolphin.py`
- Create: `tests/test_dolphin_scripts.py`
- Create: `tests/integration/test_stream_schema.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_dolphin_scripts.py
from tradeflow.stream.schema import TICK


def test_ensure_script_recreate_contains_drop():
    script = TICK.ensure_script(recreate=True)
    assert "dropStreamTable(`tickStreamTable)" in script
    assert "existsStreamTable(`tickStreamTable)" in script
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_dolphin_scripts.py -q`
Expected: `AssertionError`（`drop_script` 尚未接入）

- [ ] **Step 3: 修改 `Dolphin`**

```python
import threading

class Dolphin:
    def __init__(self, ...):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self._lock = threading.RLock()
        # ... 原有 session 初始化 ...

    def newSession(self, enableAsync: bool = False):
        session = ddb.session(enableASYNC=enableAsync)
        session.connect(
            host=self.host, port=self.port,
            userid=self.userid, password=self.password,
            reconnect=True,
        )
        return session

    def runScript(self, *script, enableAsync=False, clearMemory=False):
        with self._lock:
            if enableAsync:
                self.asyncSession.run(*script, clearMemory=clearMemory)
            else:
                return self.session.run(*script, clearMemory=clearMemory)

    def prepare_streams(self, table_names, recreate=False):
        """按 schema 注册表幂等建表（recreate=True 时先删后建，会丢数据）。

        所有表合并在单次 runScript 中执行（减少网络往返，避免逐表调用时
        服务器瞬时无响应导致客户端阻塞），执行后统一校验缺表并报错。
        """
        from .schema import STREAM_TABLES
        names = list(table_names)
        unknown = [name for name in names if name not in STREAM_TABLES]
        if unknown:
            raise KeyError(f'未注册的流表: {unknown}')
        blocks = [
            f"try {{ {STREAM_TABLES[name].ensure_script(recreate=recreate)} }} "
            f"catch(ex) {{ print(\"prepare {name} failed: \" + ex) }}"
            for name in names
        ]
        checks = "\n".join(
            f'if (!{STREAM_TABLES[name].exists_script()}) {{ missing = missing + "{name}" }}'
            for name in names
        )
        script = f"""
missing = string([])
{chr(10).join(blocks)}
{checks}
missing
"""
        missing = self.runScript(script) or []
        if missing:
            raise RuntimeError(f'流表创建失败: {list(missing)}')
        print(f"prepared {len(names)} stream tables!")

    def prepare_engines(self, engine_names):
        from .engines import ENGINE_SPECS, create_engine_script
        for name in engine_names:
            self.runScript(create_engine_script(ENGINE_SPECS[name]))
            print(f"prepared engine {name}!")
```

同时把旧的 `createTickStreamTable` 等方法改成：

```python
def createTickStreamTable(self, existDrop=True):
    self.prepare_streams(["tickStreamTable"], recreate=existDrop)
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_dolphin_scripts.py -q`
Expected: `1 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/stream/dolphin.py tests/test_dolphin_scripts.py
git commit -m "feat: DolphinDB 幂等建表与独立 session"
```

---

### Task 4: 公共行情解析

**Files:**
- Create: `tradeflow/subscribe/handlers/__init__.py`
- Create: `tradeflow/subscribe/handlers/market.py`
- Create: `tests/test_handlers_market.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_handlers_market.py
from tradeflow.subscribe.handlers import dispatch


def test_tickers():
    packet = {
        "arg": {"channel": "tickers", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "instType": "SWAP", "instId": "BTC-USDT-SWAP",
            "last": "100", "lastSz": "1", "askPx": "101", "askSz": "2",
            "bidPx": "99", "bidSz": "3", "open24h": "90", "high24h": "110",
            "low24h": "80", "volCcy24h": "1000", "vol24h": "10",
            "sodUtc0": "95", "sodUtc8": "96", "ts": "1704067200000",
        }],
    }
    batches = dispatch(packet)
    assert len(batches) == 1
    assert batches[0].table == "tickStreamTable"
    assert len(batches[0].frame) == 1


def test_candle_writes_all_and_confirmed():
    packet = {
        "arg": {"channel": "candle1m", "instId": "BTC-USDT-SWAP"},
        "data": [["1704067200000", "1", "2", "0.5", "1.5", "10", "10", "15", "0"]],
    }
    batches = dispatch(packet)
    tables = {b.table for b in batches}
    assert tables == {"candleStreamTable"}


def test_liquidation_all_details():
    packet = {
        "arg": {"channel": "liquidation-orders"},
        "data": [{
            "instId": "BTC-USDT-SWAP", "instType": "SWAP", "uly": "BTC-USDT",
            "details": [
                {"bkLoss": "0", "bkPx": "100", "ccy": "USDT", "posSide": "long",
                 "side": "sell", "sz": "1", "ts": "1704067200000"},
                {"bkLoss": "0", "bkPx": "101", "ccy": "USDT", "posSide": "short",
                 "side": "buy", "sz": "2", "ts": "1704067200001"},
            ],
        }],
    }
    batches = dispatch(packet)
    assert len(batches[0].frame) == 2
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_handlers_market.py -q`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现 `market.py`**

```python
# tradeflow/subscribe/handlers/market.py
import pandas as pd

from . import Batch


def _data(packet):
    return packet.get("data") or []


def parse_tickers(packet):
    return [Batch("tickStreamTable", pd.DataFrame(_data(packet)))]


def parse_candle(packet):
    rows = []
    for item in _data(packet):
        ts, o, h, l, c, vol, vol_ccy, vol_ccy_quote, confirm = item[:9]
        rows.append({
            "ts": ts, "o": o, "h": h, "l": l, "c": c,
            "vol": vol, "volCcy": vol_ccy, "volCcyQuote": vol_ccy_quote,
            "confirm": confirm,
            "instId": packet["arg"]["instId"],
            "channel": packet["arg"]["channel"],
        })
    df = pd.DataFrame(rows)
    batches = [Batch("candleStreamTable", df)]
    confirmed = df[df["confirm"].astype(int) == 1]
    if not confirmed.empty:
        batches.append(Batch("candleConfirmStreamTable", confirmed))
    return batches


def parse_trades(packet, source="trades"):
    df = pd.DataFrame(_data(packet))
    if "count" not in df.columns:
        df["count"] = 1
    df["source"] = source
    return [Batch("tradeStreamTable", df)]


def parse_books(packet, source):
    rows = []
    for item in _data(packet):
        for side, key in (("ask", "asks"), ("bid", "bids")):
            for level, level_data in enumerate(item.get(key) or [], start=1):
                rows.append({
                    "instId": item["instId"],
                    "ts": item.get("ts"),
                    "seqId": item.get("seqId", 0),
                    "prevSeqId": item.get("prevSeqId", 0),
                    "checksum": item.get("checksum", 0),
                    "side": side,
                    "level": level,
                    "px": level_data[0],
                    "sz": level_data[1],
                    "source": source,
                })
    return [Batch("bookStreamTable", pd.DataFrame(rows))]


def parse_liquidation_orders(packet):
    rows = []
    for item in _data(packet):
        for detail in item.get("details") or []:
            rows.append({
                **detail,
                "instId": item.get("instId"),
                "instType": item.get("instType"),
                "uly": item.get("uly"),
            })
    return [Batch("liquidationorderStreamTable", pd.DataFrame(rows))]


def parse_mark_price_candle(packet):
    rows = []
    for item in _data(packet):
        ts, o, h, l, c, confirm = item[:6]
        rows.append({
            "channel": packet["arg"]["channel"],
            "instId": packet["arg"]["instId"],
            "ts": ts, "o": o, "h": h, "l": l, "c": c, "confirm": confirm,
        })
    return [Batch("markpricecandleStreamTable", pd.DataFrame(rows))]


def parse_index_candle(packet):
    rows = []
    for item in _data(packet):
        ts, o, h, l, c, confirm = item[:6]
        rows.append({
            "ts": ts, "o": o, "h": h, "l": l, "c": c, "confirm": confirm,
            "instId": packet["arg"]["instId"],
            "channel": packet["arg"]["channel"],
        })
    return [Batch("indexcandleStreamTable", pd.DataFrame(rows))]
```

其余频道用同一模式补全：

```python
def parse_open_interest(packet):
    return [Batch("openinterestStreamTable", pd.DataFrame(_data(packet)))]

def parse_funding_rate(packet):
    return [Batch("fundingrateStreamTable", pd.DataFrame(_data(packet)))]

def parse_mark_price(packet):
    return [Batch("markpriceStreamTable", pd.DataFrame(_data(packet)))]

def parse_index_tickers(packet):
    return [Batch("indextickStreamTable", pd.DataFrame(_data(packet)))]

def parse_price_limit(packet):
    return [Batch("pricelimitStreamTable", pd.DataFrame(_data(packet)))]

def parse_opt_summary(packet):
    df = pd.DataFrame(_data(packet))
    if "volLv" not in df.columns:
        df["volLv"] = 0.0
    return [Batch("optsummaryStreamTable", df)]

def parse_estimated_price(packet):
    return [Batch("estimatedpriceStreamTable", pd.DataFrame(_data(packet)))]

def parse_option_trades(packet):
    return [Batch("optiontradeStreamTable", pd.DataFrame(_data(packet)))]

def parse_status(packet):
    return [Batch("statusStreamTable", pd.DataFrame(_data(packet)))]

def parse_instruments(packet):
    return [Batch("productStreamTable", pd.DataFrame(_data(packet)))]
```

`handlers/__init__.py` 先导入解析函数，再注册：

```python
from .market import (
    parse_books, parse_candle, parse_estimated_price, parse_funding_rate,
    parse_index_candle, parse_index_tickers, parse_instruments,
    parse_liquidation_orders, parse_mark_price, parse_mark_price_candle,
    parse_open_interest, parse_option_trades, parse_opt_summary,
    parse_price_limit, parse_status, parse_tickers, parse_trades,
)
from .private import (
    parse_account, parse_account_greeks, parse_algo_order, parse_balance,
    parse_fills, parse_liquidation_warning, parse_orders, parse_positions,
)

HANDLERS = {
    "tickers": parse_tickers,
    "trades": parse_trades,
    "trades-all": lambda packet: parse_trades(packet, "trades-all"),
    "books5": lambda packet: parse_books(packet, "books5"),
    "bbo-tbt": lambda packet: parse_books(packet, "bbo-tbt"),
    "books": lambda packet: parse_books(packet, "books"),
    "open-interest": parse_open_interest,
    "funding-rate": parse_funding_rate,
    "mark-price": parse_mark_price,
    "mark-price-candle": parse_mark_price_candle,
    "index-tickers": parse_index_tickers,
    "index-candle": parse_index_candle,
    "price-limit": parse_price_limit,
    "opt-summary": parse_opt_summary,
    "estimated-price": parse_estimated_price,
    "option-trades": parse_option_trades,
    "liquidation-orders": parse_liquidation_orders,
    "status": parse_status,
    "instruments": parse_instruments,
    "candle": parse_candle,
}
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_handlers_market.py -q`
Expected: `3 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/subscribe/handlers tests/test_handlers_market.py
git commit -m "feat: 公共行情频道解析"
```

---

### Task 5: 私有频道解析

**Files:**
- Create: `tradeflow/subscribe/handlers/private.py`
- Create: `tests/test_handlers_private.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_handlers_private.py
from tradeflow.subscribe.handlers import dispatch


def test_account_details():
    packet = {
        "arg": {"channel": "account"},
        "data": [{"details": [{
            "ccy": "USDT", "eq": "100", "availBal": "80",
            "uTime": "1704067200000",
        }]}],
    }
    batches = dispatch(packet)
    assert batches[0].table == "shareAccountTable"
    assert batches[0].frame.loc[0, "ccy"] == "USDT"


def test_orders_and_snapshot():
    packet = {
        "arg": {"channel": "orders", "instType": "ANY"},
        "data": [{
            "instId": "BTC-USDT-SWAP", "ordId": "1", "uTime": "1704067200000",
            "cTime": "1704067200000", "state": "live", "sz": "1",
            "px": "100", "side": "buy", "posSide": "net",
        }],
    }
    tables = {b.table for b in dispatch(packet)}
    assert tables == {"orderStreamTable", "shareOrderTable"}


def test_algo_list_fields():
    packet = {
        "arg": {"channel": "orders-algo", "instType": "ANY"},
        "data": [{
            "instId": "BTC-USDT-SWAP", "algoId": "9",
            "ordIdList": ["1", "2"], "subAlgoIdList": ["3"],
            "cTime": "1704067200000", "uTime": "1704067200000",
        }],
    }
    batch = dispatch(packet)[0]
    assert batch.frame.loc[0, "ordIdList"] == "1,2"
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_handlers_private.py -q`
Expected: `KeyError` 或 `ModuleNotFoundError`

- [ ] **Step 3: 实现 `private.py`**

```python
# tradeflow/subscribe/handlers/private.py
import pandas as pd

from ...utils.format import json_dumps
from . import Batch


def _data(packet):
    return packet.get("data") or []


def parse_account(packet):
    rows = []
    for item in _data(packet):
        rows.extend(item.get("details") or [])
    return [Batch("shareAccountTable", pd.DataFrame(rows))]


def parse_positions(packet):
    return [Batch("sharePositionTable", pd.DataFrame(_data(packet)))]


def parse_orders(packet):
    df = pd.DataFrame(_data(packet))
    return [
        Batch("orderStreamTable", df),
        Batch("shareOrderTable", df.copy()),
    ]


def parse_fills(packet):
    return [Batch("fillsStreamTable", pd.DataFrame(_data(packet)))]


def parse_balance(packet):
    rows = []
    for item in _data(packet):
        rows.extend(item.get("balData") or [])
    return [Batch("shareBalanceTable", pd.DataFrame(rows))]


def parse_liquidation_warning(packet):
    return [Batch("shareWarningTable", pd.DataFrame(_data(packet)))]


def parse_account_greeks(packet):
    df = pd.DataFrame(_data(packet))
    return [
        Batch("accountGreeksStreamTable", df),
        Batch("shareAccountGreeksTable", df.copy()),
    ]


def parse_algo_order(packet):
    data = _data(packet)
    df = pd.DataFrame(data)
    for key in ("ordIdList", "subAlgoIdList"):
        if key in df.columns:
            df[key] = df[key].map(
                lambda value: ",".join(map(str, value)) if isinstance(value, list) else value
            )
    df["raw"] = [json_dumps(item) for item in data]
    return [
        Batch("algoOrderStreamTable", df),
        Batch("shareAlgoOrderTable", df.copy()),
    ]
```

`HANDLERS` 增加：

```python
"account": parse_account,
"positions": parse_positions,
"orders": parse_orders,
"fills": parse_fills,
"balance_and_position": parse_balance,
"liquidation-warning": parse_liquidation_warning,
"account-greeks": parse_account_greeks,
"orders-algo": parse_algo_order,
"algo-advance": parse_algo_order,
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_handlers_private.py -q`
Expected: `3 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/subscribe/handlers/private.py tradeflow/subscribe/handlers/__init__.py tests/test_handlers_private.py
git commit -m "feat: 私有频道解析"
```

---

### Task 6: 异步批量写入

**Files:**
- Create: `tradeflow/subscribe/writer.py`
- Create: `tests/test_writer.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_writer.py
import threading

import pandas as pd

from tradeflow.subscribe.writer import StreamWriter


class FakeSession:
    """可阻塞的假 session：block_first=True 时首次写入挂起，模拟写库慢。"""

    def __init__(self, block_first: bool = False):
        self.calls = []
        self.block_first = block_first
        self.first_write_started = threading.Event()
        self.release = threading.Event()

    def run(self, script, frame):
        self.calls.append((script, frame))
        if self.block_first and not self.release.is_set():
            self.first_write_started.set()
            self.release.wait(timeout=5)


class FakeDolphin:
    def __init__(self, block_first: bool = False):
        self.session = FakeSession(block_first)

    def newSession(self):
        return self.session


def test_writer_batches_by_table():
    dolphin = FakeDolphin()
    # flush_interval 加大到 0.2s，保证两帧在同一个积累窗口内合并（避免调度抖动拆批）
    writer = StreamWriter(dolphin, batch_size=10, flush_interval=0.2)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 1}]))
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 2}]))
    writer.stop()
    assert len(dolphin.session.calls) == 1
    assert len(dolphin.session.calls[0][1]) == 2


def test_writer_groups_multiple_tables():
    dolphin = FakeDolphin()
    writer = StreamWriter(dolphin, batch_size=10, flush_interval=0.2)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 1}]))
    writer.put("candleStreamTable", pd.DataFrame([{"ts": 1, "c": 1.0}]))
    writer.stop()
    scripts = [script for script, _ in dolphin.session.calls]
    assert len(scripts) == 2


def test_writer_drops_when_full():
    dolphin = FakeDolphin(block_first=True)
    writer = StreamWriter(dolphin, batch_size=1, flush_interval=1, queue_size=1)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 1}]))
    # 等写线程取走第一帧并阻塞在 session.run 内，此时队列已空且无法继续消化
    assert dolphin.session.first_write_started.wait(timeout=2)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 2}]))  # 占满队列
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 3}]))  # 必然丢弃
    assert writer.stats()["dropped"] >= 1
    dolphin.session.release.set()  # 放行写线程，恢复队列消化
    writer.stop()


def test_writer_skips_empty_frame():
    dolphin = FakeDolphin()
    writer = StreamWriter(dolphin, batch_size=10, flush_interval=0.2)
    writer.put("tickStreamTable", pd.DataFrame())
    writer.stop()
    assert dolphin.session.calls == []


def test_writer_records_written_rows():
    dolphin = FakeDolphin()
    writer = StreamWriter(dolphin, batch_size=10, flush_interval=0.2)
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "BTC", "ts": 1}]))
    writer.put("tickStreamTable", pd.DataFrame([{"instId": "ETH", "ts": 2}]))
    writer.stop()
    assert writer.stats()["written"] == 2
    assert writer.stats()["dropped"] == 0
```

> 时序说明：旧写法直接连 put 两次就断言 dropped，写线程可能抢先把第一帧消费掉，导致测试偶发通过/失败。改为"写线程阻塞在 session.run 内 + 先等 `first_write_started`"后，队列状态是确定的：第一帧在 handler 里挂起、第二帧占满唯一槽位、第三帧必然溢出。`block_first=False` 的用例不阻塞，避免 `stop()` 等待超时。

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_writer.py -q`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现 `writer.py`**

按第 6.6 节实现；`_flush` 关键逻辑：

```python
def _flush(self, batch):
    groups = defaultdict(list)
    for table, frame in batch:
        groups[table].append(frame)
    for table, frames in groups.items():
        frame = pd.concat(frames, ignore_index=True)
        spec = STREAM_TABLES.get(table)
        if spec is None:
            self._errors += len(frame)
            logger.error(f"未知流表 {table}，丢弃 {len(frame)} 行")
            continue
        frame = spec.normalize(frame)
        try:
            self._session.run(f"tableInsert{{{table}}}", frame)
            self._written += len(frame)
        except Exception as exc:
            self._errors += len(frame)
            logger.exception(f"写入 {table} 失败: {exc}")
            for start in range(0, len(frame), 100):
                chunk = frame.iloc[start:start + 100]
                try:
                    self._session.run(f"tableInsert{{{table}}}", chunk)
                    self._written += len(chunk)
                    self._errors -= len(chunk)
                except Exception as chunk_exc:
                    logger.error(f"{table} 分块重试仍失败: {chunk_exc}")
```

`stop()` 设置 `_stop`，`join(timeout=5)`，最后 `_flush` 剩余队列。

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_writer.py -q`
Expected: `2 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/subscribe/writer.py tests/test_writer.py
git commit -m "feat: 行情异步批量落库"
```

---

### Task 7: WebSocket 连接基类

**Files:**
- Create: `tradeflow/subscribe/base.py`
- Create: `tests/test_subscribe_base.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_subscribe_base.py
import pytest

from tradeflow.subscribe.base import Subscribe


class Dummy(Subscribe):
    endpoint = "public"
    login_required = False

    def default_url(self):
        return "wss://example.invalid"


def test_dynamic_subscribe_updates_desired():
    sub = Dummy(None, [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}], writer=None)
    assert len(sub.desired) == 1
    sub.add_desired([{"channel": "tickers", "instId": "ETH-USDT-SWAP"}])
    assert len(sub.desired) == 2


def test_error_event_isolated():
    sub = Dummy(None, [{"channel": "books5", "instId": "BTC-USDT-SWAP"}], writer=None)
    sub._handle_event({
        "event": "error", "code": "60012",
        "msg": "channel does not exist",
        "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
    })
    assert len(sub.desired) == 0
    assert sub.error_count == 1
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_subscribe_base.py -q`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现 `base.py`**

按第 6.5 节实现，并补两个同步辅助方法：

```python
@property
def desired(self):
    return self._desired

def add_desired(self, args):
    for arg in args:
        self._desired[self._key(arg)] = arg

def remove_desired(self, args):
    for arg in args:
        self._desired.pop(self._key(arg), None)
```

`_key` 使用 `json.dumps(arg, sort_keys=True)`。

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_subscribe_base.py -q`
Expected: `2 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/subscribe/base.py tests/test_subscribe_base.py
git commit -m "feat: WebSocket 连接基类"
```

---

### Task 8: 频道清单、连接类与 Kernel 集成

**Files:**
- Create: `tradeflow/subscribe/channels.py`
- Create: `tradeflow/subscribe/public.py`
- Create: `tradeflow/subscribe/private.py`
- Create: `tradeflow/subscribe/manager.py`
- Create: `tradeflow/subscribe/__init__.py`
- Modify: `tradeflow/kernel.py`
- Create: `tests/test_kernel_channels.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_kernel_channels.py
from tradeflow.kernel import Kernel


def test_default_public_channels():
    kernel = Kernel(flag="1", private=False, instIds={"BTC-USDT-SWAP"})
    channels = kernel.publicChannels
    names = {item["channel"] for item in channels}
    assert "tickers" in names
    assert "funding-rate" in names
    assert "index-tickers" in names


def test_depth_toggle():
    kernel = Kernel(flag="1", private=False, instIds={"BTC-USDT-SWAP"}, depth="books5")
    names = {item["channel"] for item in kernel.publicChannels}
    assert "books5" in names
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_kernel_channels.py -q`
Expected: `AssertionError`（旧 `publicChannels` 只有 tickers）

- [ ] **Step 3: 实现 `channels.py`**

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class ChannelSpec:
    name: str
    endpoint: str
    tables: tuple[str, ...]
    arg: str = "instId"
    auth: bool = False
    default: bool = False


CHANNEL_SPECS = {
    "tickers": ChannelSpec("tickers", "public", ("tickStreamTable",)),
    "trades": ChannelSpec("trades", "public", ("tradeStreamTable",)),
    "books5": ChannelSpec("books5", "public", ("bookStreamTable",)),
    "bbo-tbt": ChannelSpec("bbo-tbt", "public", ("bookStreamTable",)),
    "books": ChannelSpec("books", "public", ("bookStreamTable",)),
    "open-interest": ChannelSpec("open-interest", "public", ("openinterestStreamTable",)),
    "funding-rate": ChannelSpec("funding-rate", "public", ("fundingrateStreamTable",)),
    "mark-price": ChannelSpec("mark-price", "public", ("markpriceStreamTable",)),
    "index-tickers": ChannelSpec("index-tickers", "public", ("indextickStreamTable",), arg="indexId"),
    "price-limit": ChannelSpec("price-limit", "public", ("pricelimitStreamTable",)),
    "opt-summary": ChannelSpec("opt-summary", "public", ("optsummaryStreamTable",), arg="instFamily"),
    "estimated-price": ChannelSpec("estimated-price", "public", ("estimatedpriceStreamTable",), arg="instType"),
    "option-trades": ChannelSpec("option-trades", "public", ("optiontradeStreamTable",), arg="instFamily"),
    "liquidation-orders": ChannelSpec("liquidation-orders", "public", ("liquidationorderStreamTable",), arg="instType"),
    "status": ChannelSpec("status", "public", ("statusStreamTable",), arg="none"),
    "instruments": ChannelSpec("instruments", "public", ("productStreamTable",), arg="instType"),
    "candle1m": ChannelSpec("candle1m", "business", ("candleStreamTable", "candleConfirmStreamTable")),
    "candle5m": ChannelSpec("candle5m", "business", ("candleStreamTable", "candleConfirmStreamTable")),
    "mark-price-candle": ChannelSpec("mark-price-candle", "business", ("markpricecandleStreamTable",)),
    "index-candle": ChannelSpec("index-candle", "business", ("indexcandleStreamTable",), arg="indexId"),
    "trades-all": ChannelSpec("trades-all", "business", ("tradeStreamTable",)),
    "account": ChannelSpec("account", "private", ("shareAccountTable",), arg="none", auth=True),
    "positions": ChannelSpec("positions", "private", ("sharePositionTable",), arg="instType", auth=True),
    "orders": ChannelSpec("orders", "private", ("orderStreamTable", "shareOrderTable"), arg="instType", auth=True),
    "fills": ChannelSpec("fills", "private", ("fillsStreamTable",), arg="instType", auth=True),
    "balance_and_position": ChannelSpec("balance_and_position", "private", ("shareBalanceTable",), arg="none", auth=True),
    "liquidation-warning": ChannelSpec("liquidation-warning", "private", ("shareWarningTable",), arg="instType", auth=True),
    "account-greeks": ChannelSpec("account-greeks", "private", ("accountGreeksStreamTable", "shareAccountGreeksTable"), arg="ccy", auth=True),
    "orders-algo": ChannelSpec("orders-algo", "business-private", ("algoOrderStreamTable", "shareAlgoOrderTable"), arg="instType", auth=True),
    "algo-advance": ChannelSpec("algo-advance", "business-private", ("algoOrderStreamTable", "shareAlgoOrderTable"), arg="instType", auth=True),
}


def get_channel_spec(name):
    if name.startswith("mark-price-candle"):
        return CHANNEL_SPECS["mark-price-candle"]
    if name.startswith("index-candle"):
        return CHANNEL_SPECS["index-candle"]
    if name.startswith("candle"):
        return CHANNEL_SPECS["candle1m"]
    return CHANNEL_SPECS.get(name)


def tables_for(channels):
    tables = set()
    for name in channels:
        spec = get_channel_spec(name)
        if spec:
            tables.update(spec.tables)
    return tables
```

- [ ] **Step 4: 实现连接类和 manager**

`public.py`：

```python
from .base import Subscribe
from ..setting import REAL_PUBLIC_URL, SIMULATE_PUBLIC_URL, REAL_CANDLE_URL, SIMULATE_CANDLE_URL


class PublicSubscribe(Subscribe):
    endpoint = "public"

    def default_url(self):
        return REAL_PUBLIC_URL if self.kernel.flag == "0" else SIMULATE_PUBLIC_URL


class CandleSubscribe(Subscribe):
    endpoint = "business"

    def default_url(self):
        return REAL_CANDLE_URL if self.kernel.flag == "0" else SIMULATE_CANDLE_URL
```

`private.py`：

```python
class PrivateSubscribe(Subscribe):
    endpoint = "private"
    login_required = True

    def default_url(self):
        return REAL_PRIVATE_URL if self.kernel.flag == "0" else SIMULATE_PRIVATE_URL


class BusinessPrivateSubscribe(PrivateSubscribe):
    endpoint = "business-private"

    def default_url(self):
        return REAL_CANDLE_URL if self.kernel.flag == "0" else SIMULATE_CANDLE_URL
```

`manager.py`：

```python
class SubscribeManager:
    def __init__(self, kernel):
        self.kernel = kernel
        self.writer = StreamWriter(kernel.dolphin)
        self.public = PublicSubscribe(kernel, kernel.publicChannels, writer=self.writer, reconciler=kernel.reconciler)
        self.business = CandleSubscribe(kernel, kernel.candleChannels, writer=self.writer, reconciler=kernel.reconciler)
        self.private = None
        self.algo = None
        self._tasks = []

    def start(self):
        self._tasks.append(asyncio.run_coroutine_threadsafe(self.public.run(), self.kernel._loop))
        self._tasks.append(asyncio.run_coroutine_threadsafe(self.business.run(), self.kernel._loop))
        if self.kernel.private_enabled:
            self.private = PrivateSubscribe(
                self.kernel, self.kernel.privateChannels,
                writer=self.writer, reconciler=self.kernel.reconciler,
            )
            self._tasks.append(asyncio.run_coroutine_threadsafe(self.private.run(), self.kernel._loop))
        if self.kernel.algo_enabled:
            self.algo = BusinessPrivateSubscribe(
                self.kernel, self.kernel.algoChannels,
                writer=self.writer, reconciler=self.kernel.reconciler,
            )
            self._tasks.append(asyncio.run_coroutine_threadsafe(self.algo.run(), self.kernel._loop))

    def subscribe(self, endpoint, args):
        connection = {
            "public": self.public,
            "business": self.business,
            "private": self.private,
            "business-private": self.algo,
        }[endpoint]
        return asyncio.run_coroutine_threadsafe(connection.subscribe(args), self.kernel._loop)

    def health(self):
        return {
            "public": self.public.health(),
            "business": self.business.health(),
            "private": self.private.health() if self.private else None,
            "algo": self.algo.health() if self.algo else None,
            "writer": self.writer.stats(),
        }

    def stop(self):
        for connection in (self.public, self.business, self.private, self.algo):
            if connection is not None:
                connection.active = False
        for task in self._tasks:
            task.cancel()
        self.writer.stop()
```

`Kernel` 增加 `market/depth/options/algo/recreate_tables` 参数、`enabled_channels()`、`publicChannels`/`candleChannels`/`privateChannels`/`algoChannels`、`prepare()` 新逻辑、`subscribe()` 新逻辑、`stop()` 新逻辑。按第 7 节实现。

- [ ] **Step 5: 运行测试**

Run: `pytest tests/test_kernel_channels.py -q`
Expected: `2 passed`

- [ ] **Step 6: 提交**

```bash
git add tradeflow/subscribe tradeflow/kernel.py tests/test_kernel_channels.py
git commit -m "feat: 订阅模块化与 Kernel 频道编排"
```

---

### Task 9: 流引擎与 DolphinDB 集成测试

**Files:**
- Create: `tradeflow/stream/engines.py`
- Modify: `tradeflow/stream/dolphin.py`
- Create: `tests/integration/test_stream_schema.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/integration/test_stream_schema.py
import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("TRADEFLOW_TEST_DDB") != "1"
    or os.getenv("TRADEFLOW_TEST_DDB_ALLOW_DROP") != "1",
    reason="需要独立的 DolphinDB 测试实例，并显式允许删表",
)


def test_stream_tables_engines_and_dataflow():
    from tradeflow.stream.dolphin import Dolphin
    from tradeflow.stream.engines import ENGINE_SPECS
    from tradeflow.stream.schema import STREAM_TABLES

    # 该测试会 drop/recreate 全部流表，必须指向独立测试实例。
    # 关键顺序（实测）：先摘除引擎订阅，再建表，最后重建引擎——
    # 引擎持有输出表引用，活跃订阅会阻塞 dropStreamTable。
    d = Dolphin()
    for spec in ENGINE_SPECS.values():
        d.unsubscribeTable(spec.source, spec.action)
        d.dropStreamEngine(spec.name)
    d.prepare_streams(STREAM_TABLES.keys(), recreate=True)
    d.prepare_engines((
        "lastTickAction", "lastCandleAction",
        "candleIndicatorEngine", "tickIndicatorEngine",
        "lastTickIndicatorAction", "lastCandleIndicatorAction",
    ))
    time.sleep(1)

    # 表与快照表存在
    assert d.runScript("existsStreamTable(`tickStreamTable)") is True
    assert d.runScript("defined(`shareCandleIndicatorTable, SHARED)") is True
    # 订阅注册在 getStreamingStat().pubTables（actions 列）
    pubs = d.runScript("getStreamingStat().pubTables")
    assert {"lastTickAction", "lastCandleAction", "candleIndicatorAction",
            "tickIndicatorAction"} <= set(pubs["actions"])

    # 响应式状态引擎：插入 confirm=1 的 K 线，断言 ma5 输出（minPeriods=1 首根即出值）
    d.runScript("""
    insert into candleConfirmStreamTable values(2024.01.01T00:01:00.000, 1, 2, 0.5, 1.5, 10, 10, 15, 1, "BTC-USDT-SWAP", "candle1m")
    insert into candleConfirmStreamTable values(2024.01.01T00:02:00.000, 1.5, 2.5, 1, 2, 11, 11, 16, 1, "BTC-USDT-SWAP", "candle1m")
    insert into candleConfirmStreamTable values(2024.01.01T00:03:00.000, 2, 3, 1.5, 2.5, 12, 12, 17, 1, "BTC-USDT-SWAP", "candle1m")
    """)
    time.sleep(2)
    ind = d.runScript("select * from candleIndicatorStreamTable order by ts")
    assert len(ind) == 3
    assert ind.loc[0, "ma5"] == pytest.approx(1.5)
    assert list(ind.columns[:4]) == ["instId", "channel", "ts", "ma5"]

    # 时间序列引擎与快照动作断言见 tests/integration/test_stream_schema.py 完整版
```

> 集成测试另含：`lastCandleAction` 快照断言、tick 5 分钟窗口输出断言
> （`useWindowStartTime=false` 输出窗口结束时间）、`shareTickTable` 单行最新、
> `shareTickIndicatorTable` 等于最新窗口（而非聚合窗口）。

- [ ] **Step 2: 运行并确认失败**

Run: `TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_stream_schema.py -q`
Expected: `ModuleNotFoundError: tradeflow.stream.engines`

- [ ] **Step 3: 实现 `engines.py`**

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class EngineSpec:
    name: str
    source: str
    action: str
    target: str
    kind: str  # subscription / reactive / timeseries
    metrics: str = ""
    key_columns: tuple[str, ...] = ()
    time_column: str = "ts"
    window_size: int = 0
    step: int = 0


ENGINE_SPECS = {
    "lastTickAction": EngineSpec(
        "lastTickAction", "tickStreamTable", "lastTickAction",
        "shareTickTable", "subscription",
    ),
    "lastCandleAction": EngineSpec(
        "lastCandleAction", "candleStreamTable", "lastCandleAction",
        "shareCandleTable", "subscription",
    ),
    "candleIndicatorEngine": EngineSpec(
        "candleIndicatorEngine", "candleConfirmStreamTable", "candleIndicatorAction",
        "candleIndicatorStreamTable", "reactive",
        metrics="""<[
            ts,
            mavg(c, 5, 1) as ma5,
            mavg(c, 10, 1) as ma10,
            mavg(c, 20, 1) as ma20,
            ema(c, 12) as ema12,
            ema(c, 26) as ema26,
            ema(c, 12) - ema(c, 26) as macd,
            mstd(c, 20, 1) as mstd20,
            mavg(vol, 5, 1) as volMa5,
            mmax(h, 20, 1) as high20,
            mmin(l, 20, 1) as low20
        ]>""",
        key_columns=("instId", "channel"),
    ),
    "tickIndicatorEngine": EngineSpec(
        "tickIndicatorEngine", "tickStreamTable", "tickIndicatorAction",
        "tickIndicatorStreamTable", "timeseries",
        metrics="""<[
            count(*) as count5m,
            first(last) as open5m,
            max(last) as high5m,
            min(last) as low5m,
            last(last) as close5m,
            avg(last) as avgPx,
            sum(lastSz) as vol5m
        ]>""",
        key_columns=("instId",),
        window_size=300000,
        step=60000,
    ),
    "lastOrderAction": EngineSpec(
        "lastOrderAction", "orderStreamTable", "lastOrderAction",
        "shareOrderTable", "subscription",
    ),
    "lastAlgoOrderAction": EngineSpec(
        "lastAlgoOrderAction", "algoOrderStreamTable", "lastAlgoOrderAction",
        "shareAlgoOrderTable", "subscription",
    ),
}


def create_engine_script(spec: EngineSpec) -> str:
    unsubscribe = (
        f'try {{ unsubscribeTable(tableName=`{spec.source}, '
        f'actionName="{spec.action}") }} catch(ex) {{}}'
    )
    keys = "`" + "`".join(spec.key_columns) if spec.key_columns else ""
    if spec.kind == "subscription":
        return f"""
{unsubscribe}
subscribeTable(
    tableName=`{spec.source},
    actionName="{spec.action}",
    offset=0,
    handler=tableInsert{{{spec.target}}},
    msgAsTable=true
)
"""
    if spec.kind == "reactive":
        return f"""
{unsubscribe}
try {{ dropStreamEngine(`{spec.name}) }} catch(ex) {{}}
{spec.name} = createReactiveStateEngine(
    name="{spec.name}",
    metrics={spec.metrics},
    dummyTable={spec.source},
    outputTable={spec.target},
    keyColumn={keys}
)
subscribeTable(
    tableName=`{spec.source},
    actionName="{spec.action}",
    offset=0,
    handler=tableInsert{{{spec.name}}},
    msgAsTable=true
)
"""
    return f"""
{unsubscribe}
try {{ dropStreamEngine(`{spec.name}) }} catch(ex) {{}}
{spec.name} = createTimeSeriesEngine(
    name="{spec.name}",
    windowSize={spec.window_size},
    step={spec.step},
    metrics={spec.metrics},
    dummyTable={spec.source},
    outputTable={spec.target},
    timeColumn=`{spec.time_column},
    useSystemTime=false,
    keyColumn={keys},
    useWindowStartTime=false,
    fill="none"
)
subscribeTable(
    tableName=`{spec.source},
    actionName="{spec.action}",
    offset=0,
    handler=tableInsert{{{spec.name}}},
    msgAsTable=true
)
"""
```

- [ ] **Step 4: 运行集成测试**

Run: `TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_stream_schema.py -q -s`
Expected: `1 passed`，输出中能看到 `prepared tickStreamTable!`、`prepared engine candleIndicatorEngine!`。

- [ ] **Step 5: 提交**

```bash
git add tradeflow/stream/engines.py tradeflow/stream/dolphin.py tests/integration/test_stream_schema.py
git commit -m "feat: DolphinDB 流引擎定义与集成测试"
```

---

### Task 10: 断线对账与 K 线补齐

**Files:**
- Create: `tradeflow/subscribe/reconcile.py`
- Create: `tests/test_reconcile.py`
- Modify: `tradeflow/kernel.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_reconcile.py
from tradeflow.subscribe.reconcile import Reconciler


class FakeAPI:
    def __init__(self):
        self.trade = self
        self.account = self
        self.market = self
        self.calls = []

    def get_order_list(self, **kwargs):
        self.calls.append(("orders", kwargs))
        return {"code": "0", "data": []}

    def get_positions(self, **kwargs):
        self.calls.append(("positions", kwargs))
        return {"code": "0", "data": []}

    def get_account_balance(self, **kwargs):
        self.calls.append(("account", kwargs))
        return {"code": "0", "data": []}


class FakeKernel:
    def __init__(self):
        self.api = FakeAPI()
        self.instIds = set()


def test_private_reconnect_syncs_three_sources():
    kernel = FakeKernel()
    Reconciler(kernel).after_reconnect("private")
    assert {call[0] for call in kernel.api.calls} == {"orders", "positions", "account"}
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_reconcile.py -q`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现 `reconcile.py`**

按第 6.7 节实现，并加：

```python
def backfill_candles(self):
    for instId in self.kernel.instIds:
        last = self.kernel.dolphin.runScript(
            f"exec max(ts) from candleStreamTable where instId='{instId}'"
        )
        if last is None:
            continue
        packet = self.kernel.api.market.get_history_candlesticks(
            instId, after="", before="", bar="1m", limit="300"
        )
        rows = packet.get("data") or []
        if not rows:
            continue
        df = pd.DataFrame(rows, columns=[
            "ts", "o", "h", "l", "c", "vol", "volCcy",
            "volCcyQuote", "confirm",
        ])
        df["instId"] = instId
        df["channel"] = "candle1m"
        frame = STREAM_TABLES["candleStreamTable"].normalize(df)
        self.kernel.dolphin.data2MemoryTable("candleStreamTable", frame)
```

`Kernel.__init__` 增加 `self.reconciler = Reconciler(self)`，并把 `reconciler` 传给各连接。

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_reconcile.py -q`
Expected: `1 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/subscribe/reconcile.py tradeflow/kernel.py tests/test_reconcile.py
git commit -m "feat: 断线对账与 K 线补齐"
```

---

### Task 11: 动态订阅与健康指标

**Files:**
- Modify: `tradeflow/subscribe/base.py`
- Modify: `tradeflow/subscribe/manager.py`
- Modify: `tradeflow/kernel.py`
- Create: `tests/test_health.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_health.py
from tradeflow.subscribe.base import Subscribe


class Dummy(Subscribe):
    endpoint = "public"
    login_required = False

    def default_url(self):
        return "wss://example.invalid"


def test_health_shape():
    sub = Dummy(None, [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}], writer=None)
    health = sub.health()
    assert health["connected"] is False
    assert health["channels"] == ["tickers:BTC-USDT-SWAP"]
    assert health["reconnect_count"] == 0
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_health.py -q`
Expected: `AttributeError`

- [ ] **Step 3: 实现健康指标**

`Subscribe.health()`：

```python
def health(self):
    return {
        "endpoint": self.endpoint,
        "connected": self._connected,
        "last_message_at": self._last_message_at,
        "reconnect_count": self._reconnect_count,
        "error_count": self._error_count,
        "request_count": self._request_count,
        "channels": [
            f"{arg.get('channel')}:{arg.get('instId') or arg.get('instType') or ''}"
            for arg in self._desired.values()
        ],
    }
```

`StreamWriter.stats()`：

```python
def stats(self):
    return {
        "queue": self._queue.qsize(),
        "written": self._written,
        "dropped": self._dropped,
        "errors": self._errors,
    }
```

`Kernel.health()`：

```python
def health(self):
    return self.manager.health() if self.manager is not None else {}
```

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_health.py -q`
Expected: `1 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/subscribe/base.py tradeflow/subscribe/manager.py tradeflow/kernel.py tests/test_health.py
git commit -m "feat: 订阅健康指标"
```

---

### Task 12: 增量盘口 `books` 与序号校验（P1）

**Files:**
- Create: `tradeflow/subscribe/orderbook.py`
- Modify: `tradeflow/subscribe/handlers/market.py`
- Create: `tests/test_orderbook.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/test_orderbook.py
from tradeflow.subscribe.orderbook import OrderBook


def test_apply_snapshot_and_delta():
    book = OrderBook()
    book.apply_snapshot("BTC", [[100.0, 1.0], [99.0, 2.0]], [[101.0, 1.0]], seq_id=10)
    assert book.best_bid("BTC") == 100.0
    assert book.best_ask("BTC") == 101.0
    book.apply_delta("BTC", [[100.0, 0.0]], [], prev_seq_id=10, seq_id=11)
    assert book.best_bid("BTC") == 99.0


def test_gap_detected():
    book = OrderBook()
    book.apply_snapshot("BTC", [[100.0, 1.0]], [[101.0, 1.0]], seq_id=10)
    assert book.apply_delta("BTC", [], [], prev_seq_id=12, seq_id=13) is False
```

- [ ] **Step 2: 运行并确认失败**

Run: `pytest tests/test_orderbook.py -q`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现 `orderbook.py`**

```python
class OrderBook:
    def __init__(self):
        self._books = {}

    def apply_snapshot(self, inst_id, bids, asks, seq_id):
        self._books[inst_id] = {
            "bids": {float(px): float(sz) for px, sz in bids if float(sz) > 0},
            "asks": {float(px): float(sz) for px, sz in asks if float(sz) > 0},
            "seq_id": int(seq_id),
        }

    def apply_delta(self, inst_id, bids, asks, prev_seq_id, seq_id):
        book = self._books.get(inst_id)
        if book is None or int(prev_seq_id) != book["seq_id"]:
            return False
        for side, levels in (("bids", bids), ("asks", asks)):
            for px, sz in levels:
                px, sz = float(px), float(sz)
                if sz == 0:
                    book[side].pop(px, None)
                else:
                    book[side][px] = sz
        book["seq_id"] = int(seq_id)
        return True

    def best_bid(self, inst_id):
        return max(self._books[inst_id]["bids"]) if self._books[inst_id]["bids"] else None

    def best_ask(self, inst_id):
        return min(self._books[inst_id]["asks"]) if self._books[inst_id]["asks"] else None
```

`parse_books` 对 `books` 频道使用 `OrderBook`；序号不连续时返回空批次并记录日志，由 `Reconciler.backfill_orderbook(instId)` 调 REST `get_orderbook(instId, sz=400)` 重建快照。

- [ ] **Step 4: 运行测试**

Run: `pytest tests/test_orderbook.py -q`
Expected: `2 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/subscribe/orderbook.py tradeflow/subscribe/handlers/market.py tests/test_orderbook.py
git commit -m "feat: 增量盘口与序号校验"
```

---

### Task 13: DFS 历史归档（P1）

**Files:**
- Create: `tradeflow/stream/archive.py`
- Create: `tests/integration/test_archive.py`

- [ ] **Step 1: 写失败测试**

```python
# tests/integration/test_archive.py
import os

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("TRADEFLOW_TEST_DDB") != "1",
    reason="需要本地 DolphinDB",
)


def test_archive_subscription_created():
    from tradeflow.stream.archive import ensure_archive
    from tradeflow.stream.dolphin import Dolphin

    ensure_archive()
    workers = Dolphin().runScript("getStreamingStat().subWorkers")
    assert any("archiveTick" in str(row) for row in workers.to_dict("records"))


def test_archive_writes_to_dfs():
    from tradeflow.stream.archive import ARCHIVE_DB, ensure_archive
    from tradeflow.stream.dolphin import Dolphin

    d = Dolphin()
    ensure_archive(d)
    # 直接调用归档 handler 验证转换逻辑，不向流表写测试数据
    d.runScript("""
    msg = table(2030.12.31T00:00:00.000 as ts, "ZZ-ARCHIVE-TEST" as instId, 100.0 as last, 1.0 as lastSz)
    archiveTick(msg)
    """)
    count = d.runScript(
        f'exec count(*) from loadTable("{ARCHIVE_DB}", `tick) '
        'where date = 2030.12.31 and instId = "ZZ-ARCHIVE-TEST"'
    )
    assert count == 1
    d.runScript(
        f'delete from loadTable("{ARCHIVE_DB}", `tick) '
        'where date = 2030.12.31 and instId = "ZZ-ARCHIVE-TEST"'
    )
```

> 第二个测试在 `dfs://market` 写入并在结尾删除一条合成数据（`2030.12.31` + 特殊 instId），不污染流表。

- [ ] **Step 2: 运行并确认失败**

Run: `TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_archive.py -q`
Expected: `ModuleNotFoundError`

- [ ] **Step 3: 实现 `archive.py`**

> **已在本机 DolphinDB 3.00.6 端到端实测**（建库 → handler 转换 → 订阅 → 写入 → 查询 → 清理全部通过），以下写法勿再"优化"：
>
> 1. COMPO 分区至少需要两个子域（VALUE(date) + HASH(instId)），单个子域会报错；
> 2. 必须用函数式 `createPartitionedTable(db, t, ...)`——`db.createPartitionedTable(...)` 作为独立语句在该版本解析失败；
> 3. handler 函数体内**不能引用全局变量**（如 `dbPath`），路径必须写字面量；
> 4. DFS 分区表第一列是 `date(ts)`，流表没有 `date` 列，因此不能用 `append!{loadTable(...)}` 直接追加，必须先 `select date(ts) as date` 转换再 `tableInsert`；
> 5. 查询归档表必须带 `date` 过滤，否则分区数过多直接报错；
> 6. handler 定义与引用它的 `subscribeTable` 放在同一个脚本块内。

```python
# tradeflow/stream/archive.py
"""流表 -> DFS 历史归档（P1）。

归档表只存策略回测需要的核心列，避免重复存全量 63 列订单。
写法细节见模块 docstring 与上方实测说明。
"""

ARCHIVE_DB = "dfs://market"

ARCHIVE_DB_SCRIPT = """
dbDate = database("", VALUE, 2024.01.01..2030.12.31)
dbHash = database("", HASH, [SYMBOL, 30])
db = database("dfs://market", COMPO, [dbDate, dbHash])
if (!existsTable("dfs://market", `tick)) {
    t = table(1:0, `date`instId`ts`last`lastSz, [DATE,SYMBOL,TIMESTAMP,DOUBLE,DOUBLE])
    createPartitionedTable(db, t, `tick, `date`instId)
}
if (!existsTable("dfs://market", `trade)) {
    t = table(1:0, `date`instId`ts`px`sz, [DATE,SYMBOL,TIMESTAMP,DOUBLE,DOUBLE])
    createPartitionedTable(db, t, `trade, `date`instId)
}
"""

# (流表, action, DFS 表名, select 列，顺序与 DFS 表列一致)
ARCHIVE_RULES = (
    ("tickStreamTable", "archiveTick", "tick", "instId, ts, last, lastSz"),
    ("tradeStreamTable", "archiveTrade", "trade", "instId, ts, px, sz"),
)


def ensure_archive(dolphin=None) -> None:
    from .dolphin import Dolphin

    d = dolphin or Dolphin()
    d.runScript(ARCHIVE_DB_SCRIPT)
    for table, action, target, cols in ARCHIVE_RULES:
        # handler 定义与 subscribeTable 必须在同一个脚本块内，
        # 且函数体只能用路径字面量，不能引用全局变量。
        d.runScript(f"""
        def {action}(msg) {{
            tableInsert(loadTable("{ARCHIVE_DB}", `{target}), select date(ts) as date, {cols} from msg)
        }}
        try {{ unsubscribeTable(tableName=`{table}, actionName="{action}") }} catch(ex) {{}}
        subscribeTable(
            tableName=`{table},
            actionName="{action}",
            offset=0,
            handler={action},
            msgAsTable=true,
            batchSize=10000,
            throttle=5,
            persistOffset=true
        )
        """)
```

> `candle` 归档可按同样模式追加到 `ARCHIVE_RULES`（`date`instId`ts`o`h`l`c`vol`confirm + `channel`），列顺序与 DFS 表定义保持一致即可。

- [ ] **Step 4: 运行测试**

Run: `TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_archive.py -q`
Expected: `2 passed`

- [ ] **Step 5: 提交**

```bash
git add tradeflow/stream/archive.py tests/integration/test_archive.py
git commit -m "feat: 流表历史归档到 DFS"
```

---

### Task 14: 文档、依赖与最终验收

**Files:**
- Modify: `README.md`
- Modify: `requirements.txt`
- Modify: `pyproject.toml`
- Modify: `tradeflow/__init__.py`
- Modify: `strategy/example.py`

- [ ] **Step 1: 更新依赖**

`requirements.txt` 改为与实际 Python 3.12 环境匹配：

```text
dolphindb==3.0.6.2
httpx[http2]>=0.24.0
joblib>=1.3
loguru>=0.7.0
pandas>=2.2
psutil>=5.9
PyMySQL>=1.1
SQLAlchemy>=2.0
tqdm>=4.65
websockets>=13
```

`pyproject.toml` 版本改为 `3.4.0`，`tradeflow/__init__.py` 的 `__version__` 同步。

- [ ] **Step 2: 更新 README**

至少增加：

1. 频道矩阵（第 3.1 节）；
2. 流表/引擎矩阵（第 4、5 节）；
3. `Kernel` 新参数 `market/depth/options/algo/recreate_tables`；
4. 迁移说明：旧流表需 `TRADEFLOW_RECREATE_TABLES=1` 重建；
5. 健康检查示例：

```python
import json
print(json.dumps(kernel.health(), indent=2, ensure_ascii=False))
```

6. 测试命令（第 9.4 节）。

- [ ] **Step 3: 更新示例**

`strategy/example.py` 的 `scan()` 增加一行健康日志（每 30 秒）：

```python
if time.monotonic() - self._last_health >= 30:
    self._last_health = time.monotonic()
    self.log(f"health: {self.kernel.health()}")
```

- [ ] **Step 4: 全量测试**

Run:

```bash
pytest -q
TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_stream_schema.py tests/integration/test_archive.py -q
```

Expected: 全部通过。

- [ ] **Step 5: 实盘冒烟**

Run: `python -m strategy.example`
Expected:

- `tickStreamTable` 持续增长；
- `candleStreamTable` 有 1m/5m 数据；
- `tradeStreamTable` 有逐笔成交；
- `kernel.health()["public"]["error_count"] == 0`；
- 日志无 `ERROR`、无连续重连。

- [ ] **Step 6: 提交**

```bash
git add README.md requirements.txt pyproject.toml tradeflow/__init__.py strategy/example.py
git commit -m "docs: 行情订阅升级说明与 3.4.0"
```

---

## 11. 验收标准

### 11.1 功能验收

- [ ] `Kernel(private=False)` 启动后，`tickStreamTable`、`tradeStreamTable`、`candleStreamTable`、`fundingrateStreamTable`、`markpriceStreamTable`、`indextickStreamTable`、`openinterestStreamTable` 均有数据。
- [ ] `candleStreamTable` 包含 `confirm=0` 的未完成 K 线；`candleConfirmStreamTable` 只包含 `confirm=1`。
- [ ] `shareTickTable`、`shareCandleTable`、`shareOrderTable`、`sharePositionTable` 可直接按主键查询最新快照。
- [ ] `candleIndicatorStreamTable` 和 `tickIndicatorStreamTable` 有输出；`Kernel.getTradeInstid()` 可正常排序。
- [ ] `depth="books5"` 时 `bookStreamTable` 有 10 档/条数据；`bbo-tbt` 只有 1 档。
- [ ] `algo=True` 时 `algoOrderStreamTable` 能收到策略单推送。
- [ ] 动态订阅 `ETH-USDT-SWAP` 后，不重启进程即可看到新 tick。

### 11.2 稳定性验收

- [ ] 断网 5 秒后自动重连，`reconnect_count >= 1`，恢复后数据继续写入。
- [ ] 私有连接重连后 `shareOrderTable` / `sharePositionTable` 与 REST 查询一致。
- [ ] 单频道错误（如 `books5` 无权限、`fills` 非 VIP4）只禁用该频道，不影响 ticker/K 线。
- [ ] 写队列在 1000 条/秒压力下不阻塞事件循环；`writer.stats()["queue"]` 不持续增长。
- [ ] 运行 30 分钟无 `ERROR`、无内存持续上涨、无重复订阅。

### 11.3 代码质量验收

- [ ] `pytest -q` 全部通过。
- [ ] 新增模块有类型标注和 docstring。
- [ ] `subscribe/` 不直接 import `Kernel`（避免循环依赖），只通过 `kernel` 对象访问 `api/dolphin/flag/instIds`。
- [ ] `handlers/` 不直接 import `Dolphin`，只返回 `Batch`。
- [ ] `stream/schema.py` 是唯一列定义来源；`script.py` 不再出现手写 20 份 `colName/colType`。

---

## 12. 风险与回滚

| 风险 | 影响 | 缓解 |
|---|---|---|
| 重建流表导致内存数据丢失 | 开发环境可接受，生产不可接受 | `recreate_tables=False` 默认；生产先归档再重建；启动时 schema 不一致直接报错 |
| DolphinDB 引擎 metrics 写法不兼容当前版本 | 引擎创建失败 | Task 9 集成测试覆盖；先在 `localhost:8848` 验证；失败时降级为 Python 侧指标 |
| `books` 增量乱序/丢包 | 盘口错误 | 序号校验 + REST 快照重建；默认不开启 |
| OKX 480 次/小时订阅限制 | 动态订阅被限流 | 批量订阅、指数退避、`request_count` 监控 |
| `fills` 需要 VIP4 | 订阅报 64003 | 收到 64003 自动禁用该频道，保留 `orders` |
| pandas 3 与旧代码不兼容 | 类型转换报错 | 统一走 `TableSpec.normalize`，单测覆盖 |
| 写线程与主线程共用 session | 会话串扰 | `StreamWriter` 使用 `Dolphin.newSession()` 独立连接 |

**回滚方式：**

1. 代码回滚到 `b89ad2d`；
2. 删除新增流表：`dropStreamTable` / `undef(..., SHARED)`；
3. 恢复旧 `subscribe.py`（Git 历史）；
4. 重新执行旧 `Kernel.prepare()`。

---

## 13. 执行顺序与里程碑

| 里程碑 | 包含任务 | 可交付结果 |
|---|---|---|
| M1 数据层 | Task 1–3 | 格式化工具、表结构注册表、幂等建表 |
| M2 解析层 | Task 4–5 | 公共/私有频道解析，单测通过 |
| M3 管道层 | Task 6–8 | 异步写库、连接管理、Kernel 集成 |
| M4 计算层 | Task 9–10 | 流引擎、对账补齐 |
| M5 生产化 | Task 11–14 | 健康指标、增量盘口、DFS 归档、文档验收 |

建议每个里程碑结束后运行一次：

```bash
pytest -q
TRADEFLOW_TEST_DDB=1 pytest tests/integration/test_stream_schema.py -q
python -m strategy.example
```

确认无回归后再进入下一里程碑。

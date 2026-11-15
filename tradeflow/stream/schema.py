"""DolphinDB 流数据表结构注册表。

本模块是所有流表/快照表列定义的**唯一来源**：

- ``script.py`` 的建表脚本从这里生成；
- ``subscribe`` 的 handler 通过 ``TableSpec.normalize`` 对齐列；
- 旧表列顺序保持不变，新增列一律追加在末尾（升级需重建表）。
"""
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
        var = f"t_{self.name}"
        if self.latest:
            keys = "`" + "`".join(self.key_columns)
            return (
                f"{var} = latestIndexedTable({keys}, `{self.time_column}, "
                f"{self.capacity}:0, {names}, [{types}])\n"
                f"share {var} as {self.name}"
            )
        return (
            f"{var} = streamTable({self.capacity}:0, {names}, [{types}])\n"
            f'enableTableShareAndPersistence({var}, "{self.name}", false, true, '
            f"preCache=0, cacheSize={self.capacity}, "
            f"retentionMinutes={self.retention_minutes});"
        )

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


# =====================================================================
# 1. 公共频道-行情
# =====================================================================

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
    description="ticker 实时总线",
)

SHARE_TICK = TableSpec(
    "shareTickTable",
    TICK.columns,
    key_columns=("instId",),
    time_column="ts",
    capacity=1000,
    latest=True,
    description="最新 ticker 快照",
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
    description="所有 K 线（含未完成）",
)

# 与 candleStreamTable 同结构，只喂 confirm=1 的已收盘 K 线给指标引擎
CANDLE_CONFIRM = TableSpec(
    "candleConfirmStreamTable",
    CANDLE.columns,
    capacity=500_000,
    retention_minutes=180,
    description="仅 confirm=1 的已收盘 K 线",
)

SHARE_CANDLE = TableSpec(
    "shareCandleTable",
    CANDLE.columns,
    key_columns=("instId", "channel"),
    time_column="ts",
    capacity=5000,
    latest=True,
    description="最新 K 线快照（含未完成）",
)

OPEN_INTEREST = TableSpec(
    "openinterestStreamTable",
    cols(
        "instType:SYMBOL", "instId:SYMBOL", "oi:DOUBLE", "oiCcy:DOUBLE",
        "ts:TIMESTAMP",
    ),
    capacity=100_000,
    retention_minutes=60,
    description="持仓量",
)

FUNDING = TableSpec(
    "fundingrateStreamTable",
    cols(
        "fundingRate:DOUBLE", "fundingTime:LONG", "instId:SYMBOL",
        "instType:SYMBOL", "nextFundingRate:DOUBLE", "nextFundingTime:LONG",
        # 以下为新增列（追加末尾）
        "premium:DOUBLE", "settState:SYMBOL", "settFundingRate:DOUBLE",
        "minFundingRate:DOUBLE", "maxFundingRate:DOUBLE",
        "interestRate:DOUBLE", "impactValue:DOUBLE", "method:SYMBOL",
        "formulaType:SYMBOL", "ts:TIMESTAMP",
    ),
    capacity=50_000,
    retention_minutes=1440,
    description="资金费率",
)

MARK_PRICE = TableSpec(
    "markpriceStreamTable",
    cols(
        "instType:SYMBOL", "instId:SYMBOL", "markPx:DOUBLE", "ts:TIMESTAMP",
    ),
    capacity=100_000,
    retention_minutes=1440,
    description="标记价格",
)

MARK_PRICE_CANDLE = TableSpec(
    "markpricecandleStreamTable",
    cols(
        "channel:SYMBOL", "instId:SYMBOL", "ts:TIMESTAMP",
        "o:DOUBLE", "h:DOUBLE", "l:DOUBLE", "c:DOUBLE", "confirm:INT",
    ),
    capacity=200_000,
    retention_minutes=1440,
    description="标记价格 K 线",
)

INDEX_TICK = TableSpec(
    "indextickStreamTable",
    cols(
        "instId:SYMBOL", "idxPx:DOUBLE", "high24h:DOUBLE", "low24h:DOUBLE",
        "open24h:DOUBLE", "sodUtc0:DOUBLE", "sodUtc8:DOUBLE", "ts:TIMESTAMP",
    ),
    capacity=200_000,
    retention_minutes=180,
    description="指数 ticker",
)

INDEX_CANDLE = TableSpec(
    "indexcandleStreamTable",
    cols(
        "ts:TIMESTAMP", "o:DOUBLE", "h:DOUBLE", "l:DOUBLE", "c:DOUBLE",
        "confirm:INT", "instId:SYMBOL", "channel:SYMBOL",
    ),
    capacity=100_000,
    retention_minutes=180,
    description="指数 K 线",
)

PRICE_LIMIT = TableSpec(
    "pricelimitStreamTable",
    cols(
        "instId:SYMBOL", "buyLmt:DOUBLE", "sellLmt:DOUBLE", "ts:TIMESTAMP",
    ),
    capacity=100_000,
    retention_minutes=1440,
    description="限价范围",
)

OPT_SUMMARY = TableSpec(
    "optsummaryStreamTable",
    cols(
        "instType:SYMBOL", "instId:SYMBOL", "uly:SYMBOL",
        "delta:DOUBLE", "gamma:DOUBLE", "vega:DOUBLE", "theta:DOUBLE",
        "lever:DOUBLE", "markVol:DOUBLE", "bidVol:DOUBLE", "askVol:DOUBLE",
        "realVol:DOUBLE", "deltaBS:DOUBLE", "gammaBS:DOUBLE",
        "thetaBS:DOUBLE", "vegaBS:DOUBLE", "ts:TIMESTAMP", "fwdPx:DOUBLE",
        # 新增列（追加末尾）
        "volLv:DOUBLE",
    ),
    capacity=200_000,
    retention_minutes=60,
    description="期权摘要",
)

ESTIMATED_PRICE = TableSpec(
    "estimatedpriceStreamTable",
    cols(
        "instType:SYMBOL", "instId:SYMBOL", "settlePx:DOUBLE", "ts:TIMESTAMP",
    ),
    capacity=100_000,
    retention_minutes=1440,
    description="交割/行权价格",
)

OPTION_TRADE = TableSpec(
    "optiontradeStreamTable",
    cols(
        "fillVol:DOUBLE", "fwdPx:DOUBLE", "indexPx:DOUBLE",
        "instFamily:SYMBOL", "instId:SYMBOL", "markPx:DOUBLE",
        "optType:SYMBOL", "px:DOUBLE", "side:SYMBOL", "sz:DOUBLE",
        "tradeId:LONG", "ts:TIMESTAMP",
    ),
    capacity=50_000,
    retention_minutes=180,
    description="期权成交",
)

LIQUIDATION_ORDER = TableSpec(
    "liquidationorderStreamTable",
    cols(
        "bkLoss:DOUBLE", "bkPx:DOUBLE", "ccy:SYMBOL", "posSide:SYMBOL",
        "side:SYMBOL", "sz:DOUBLE", "ts:TIMESTAMP", "instId:SYMBOL",
        "instType:SYMBOL", "uly:SYMBOL",
    ),
    capacity=10_000,
    retention_minutes=1440,
    description="公共爆仓单（多明细拆行）",
)

PRODUCT = TableSpec(
    "productStreamTable",
    cols(
        "instType:SYMBOL", "instId:SYMBOL", "instFamily:SYMBOL",
        "uly:SYMBOL", "category:STRING", "baseCcy:SYMBOL", "quoteCcy:SYMBOL",
        "settleCcy:SYMBOL", "ctVal:DOUBLE", "ctMult:DOUBLE",
        "ctValCcy:SYMBOL", "optType:SYMBOL", "stk:DOUBLE",
        "listTime:TIMESTAMP", "expTime:TIMESTAMP", "tickSz:DOUBLE",
        "lotSz:DOUBLE", "minSz:DOUBLE", "ctType:SYMBOL", "alias:SYMBOL",
        "state:SYMBOL", "maxLmtSz:DOUBLE", "maxMktSz:DOUBLE",
        "maxTwapSz:DOUBLE", "maxIcebergSz:DOUBLE", "maxTriggerSz:DOUBLE",
        "maxStopSz:DOUBLE",
    ),
    capacity=1000,
    retention_minutes=1440,
    description="产品规格（instruments 频道）",
)

STATUS = TableSpec(
    "statusStreamTable",
    cols(
        "begin:LONG", "end:LONG", "env:LONG", "href:STRING",
        "maintType:LONG", "preOpenBegin:STRING", "scheDesc:STRING",
        "serviceType:LONG", "state:SYMBOL", "system:SYMBOL",
        "title:STRING", "ts:TIMESTAMP",
    ),
    capacity=1000,
    retention_minutes=1440,
    description="系统维护状态",
)

# =====================================================================
# 2. 盘口 / 逐笔
# =====================================================================

TRADE = TableSpec(
    "tradeStreamTable",
    cols(
        "instId:SYMBOL", "tradeId:LONG", "px:DOUBLE", "sz:DOUBLE",
        "side:SYMBOL", "ts:TIMESTAMP", "count:INT", "source:SYMBOL",
    ),
    capacity=500_000,
    retention_minutes=180,
    description="公共逐笔成交（trades / trades-all）",
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
    description="盘口长表（side,level 展开）",
)

# =====================================================================
# 3. 指标输出
# =====================================================================

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
    description="K 线指标输出（响应式状态引擎）",
)

SHARE_CANDLE_INDICATOR = TableSpec(
    "shareCandleIndicatorTable",
    CANDLE_INDICATOR.columns,
    key_columns=("instId", "channel"),
    time_column="ts",
    capacity=5000,
    latest=True,
    description="最新 K 线指标",
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
    description="tick 5 分钟窗口指标（时间序列引擎）",
)

SHARE_TICK_INDICATOR = TableSpec(
    "shareTickIndicatorTable",
    TICK_INDICATOR.columns,
    key_columns=("instId",),
    time_column="ts",
    capacity=5000,
    latest=True,
    description="最新 tick 指标",
)

# =====================================================================
# 4. 私有频道
# =====================================================================

_ORDER_COLUMNS = cols(
    "accFillSz:DOUBLE", "algoClOrdId:LONG", "algoId:LONG",
    "amendResult:INT", "amendSource:INT", "attachAlgoClOrdId:LONG",
    "avgPx:DOUBLE", "cTime:TIMESTAMP", "cancelSource:INT", "category:SYMBOL",
    "ccy:SYMBOL", "clOrdId:LONG", "code:INT", "execType:SYMBOL",
    "fee:DOUBLE", "feeCcy:SYMBOL", "fillFee:DOUBLE", "fillFeeCcy:SYMBOL",
    "fillFwdPx:DOUBLE", "fillMarkPx:DOUBLE",
    "fillMarkVol:DOUBLE", "fillNotionalUsd:DOUBLE", "fillPnl:DOUBLE",
    "fillPx:DOUBLE", "fillPxUsd:DOUBLE", "fillPxVol:DOUBLE",
    "fillSz:DOUBLE", "fillTime:TIMESTAMP", "instId:SYMBOL", "instType:SYMBOL",
    "lever:INT", "msg:STRING", "notionalUsd:DOUBLE", "ordId:LONG",
    "ordType:SYMBOL", "pnl:DOUBLE", "posSide:SYMBOL", "px:DOUBLE",
    "pxType:SYMBOL", "pxUsd:DOUBLE",
    "pxVol:DOUBLE", "quickMgnType:SYMBOL", "rebate:DOUBLE",
    "rebateCcy:SYMBOL", "reduceOnly:SYMBOL", "reqId:LONG", "side:SYMBOL",
    "slOrdPx:DOUBLE", "slTriggerPx:DOUBLE", "slTriggerPxType:SYMBOL",
    "source:INT", "state:SYMBOL", "stpId:LONG", "stpMode:SYMBOL",
    "sz:DOUBLE", "tag:STRING", "tdMode:SYMBOL", "tgtCcy:SYMBOL",
    "tpOrdPx:DOUBLE", "tpTriggerPx:DOUBLE",
    "tpTriggerPxType:SYMBOL", "tradeId:LONG", "uTime:TIMESTAMP",
)

ORDER = TableSpec(
    "orderStreamTable",
    _ORDER_COLUMNS,
    capacity=200_000,
    retention_minutes=1440,
    description="订单事件历史",
)

SHARE_ORDER = TableSpec(
    "shareOrderTable",
    _ORDER_COLUMNS,
    key_columns=("ordId",),
    time_column="uTime",
    capacity=5000,
    latest=True,
    description="最新订单快照",
)

ALGO_ORDER = TableSpec(
    "algoOrderStreamTable",
    cols(
        "instType:SYMBOL", "instId:SYMBOL", "ccy:SYMBOL",
        "algoId:LONG", "algoClOrdId:STRING", "ordId:LONG",
        "ordIdList:STRING", "subAlgoIdList:STRING", "clOrdId:STRING",
        "ordType:SYMBOL", "side:SYMBOL", "posSide:SYMBOL",
        "tdMode:SYMBOL", "tgtCcy:SYMBOL", "sz:DOUBLE",
        "px:DOUBLE", "pxType:SYMBOL",
        "triggerPx:DOUBLE", "triggerPxType:SYMBOL",
        "orderPx:DOUBLE", "orderPxType:SYMBOL",
        "tpTriggerPx:DOUBLE", "tpTriggerPxType:SYMBOL", "tpOrdPx:DOUBLE",
        "slTriggerPx:DOUBLE", "slTriggerPxType:SYMBOL", "slOrdPx:DOUBLE",
        "state:SYMBOL", "notionalSz:DOUBLE",
        "amendPxOnTriggerType:INT", "cTime:TIMESTAMP", "uTime:TIMESTAMP",
        "raw:STRING",
    ),
    capacity=50_000,
    retention_minutes=1440,
    description="策略委托事件（orders-algo / algo-advance）",
)

SHARE_ALGO_ORDER = TableSpec(
    "shareAlgoOrderTable",
    ALGO_ORDER.columns,
    key_columns=("algoId",),
    time_column="uTime",
    capacity=2000,
    latest=True,
    description="最新策略委托快照",
)

FILLS = TableSpec(
    "fillsStreamTable",
    cols(
        "instId:SYMBOL", "instType:SYMBOL", "tradeId:LONG", "ordId:LONG",
        "clOrdId:STRING", "billId:LONG", "tag:STRING",
        "fillPx:DOUBLE", "fillSz:DOUBLE", "side:SYMBOL", "posSide:SYMBOL",
        "execType:SYMBOL", "feeCcy:STRING", "fee:DOUBLE", "ts:TIMESTAMP",
    ),
    capacity=100_000,
    retention_minutes=1440,
    description="成交明细（VIP4+）",
)

ACCOUNT = TableSpec(
    "shareAccountTable",
    cols(
        "availBal:DOUBLE", "availEq:DOUBLE", "borrowFroz:DOUBLE",
        "cashBal:DOUBLE", "ccy:SYMBOL", "coinUsdPrice:DOUBLE",
        "crossLiab:DOUBLE", "disEq:DOUBLE", "eq:DOUBLE", "eqUsd:DOUBLE",
        "fixedBal:DOUBLE", "frozenBal:DOUBLE", "interest:DOUBLE",
        "isoEq:DOUBLE", "isoLiab:DOUBLE", "isoUpl:DOUBLE", "liab:DOUBLE",
        "maxLoan:DOUBLE", "mgnRatio:DOUBLE", "notionalLever:DOUBLE",
        "ordFrozen:DOUBLE", "spotInUseAmt:DOUBLE", "stgyEq:DOUBLE",
        "twap:DOUBLE", "uTime:TIMESTAMP", "upl:DOUBLE", "uplLiab:DOUBLE",
    ),
    key_columns=("ccy",),
    time_column="uTime",
    capacity=500,
    latest=True,
    description="账户余额快照",
)

POSITION = TableSpec(
    "sharePositionTable",
    cols(
        "adl:INT", "availPos:DOUBLE", "avgPx:DOUBLE", "baseBal:DOUBLE",
        "baseBorrowed:DOUBLE", "baseInterest:DOUBLE", "bizRefId:DOUBLE",
        "bizRefType:DOUBLE", "cTime:TIMESTAMP", "ccy:SYMBOL",
        "closeOrderAlgo:DOUBLE", "deltaBS:DOUBLE", "deltaPA:DOUBLE",
        "fee:DOUBLE", "fundingFee:DOUBLE", "gammaBS:DOUBLE",
        "gammaPA:DOUBLE", "idxPx:DOUBLE", "imr:DOUBLE", "instId:SYMBOL",
        "instType:SYMBOL", "interest:DOUBLE", "last:DOUBLE",
        "lever:DOUBLE", "liab:DOUBLE", "liabCcy:DOUBLE", "liqPenalty:DOUBLE",
        "liqPx:DOUBLE", "margin:DOUBLE", "markPx:DOUBLE",
        "mgnMode:SYMBOL", "mgnRatio:DOUBLE", "mmr:DOUBLE",
        "notionalUsd:DOUBLE", "optVal:DOUBLE", "pTime:TIMESTAMP",
        "pendingCloseOrdLiabVal:DOUBLE", "pnl:DOUBLE", "pos:DOUBLE",
        "posCcy:DOUBLE", "posId:LONG", "posSide:SYMBOL", "quoteBal:DOUBLE",
        "quoteBorrowed:DOUBLE", "quoteInterest:DOUBLE",
        "realizedPnl:DOUBLE", "spotInUseAmt:DOUBLE", "spotInUseCcy:DOUBLE",
        "thetaBS:DOUBLE", "thetaPA:DOUBLE", "tradeId:LONG",
        "uTime:TIMESTAMP", "upl:DOUBLE", "uplLastPx:DOUBLE",
        "uplRatio:DOUBLE", "uplRatioLastPx:DOUBLE", "usdPx:DOUBLE",
        "vegaBS:DOUBLE", "vegaPA:DOUBLE",
    ),
    key_columns=("instId", "posSide"),
    time_column="pTime",
    capacity=5000,
    latest=True,
    description="持仓快照（双向持仓按 posSide 区分）",
)

BALANCE = TableSpec(
    "shareBalanceTable",
    cols(
        "cashBal:DOUBLE", "ccy:SYMBOL", "uTime:TIMESTAMP",
    ),
    key_columns=("ccy",),
    time_column="uTime",
    capacity=500,
    latest=True,
    description="余额快照（balance_and_position 频道）",
)

WARNING = TableSpec(
    "shareWarningTable",
    cols(
        "adl:INT", "availPos:DOUBLE", "avgPx:DOUBLE", "cTime:TIMESTAMP",
        "ccy:SYMBOL", "deltaBS:DOUBLE", "deltaPA:DOUBLE",
        "gammaBS:DOUBLE", "gammaPA:DOUBLE", "imr:DOUBLE",
        "instId:SYMBOL", "instType:SYMBOL", "interest:DOUBLE",
        "last:DOUBLE", "lever:DOUBLE", "liab:DOUBLE", "liabCcy:SYMBOL",
        "liqPx:DOUBLE", "markPx:DOUBLE", "margin:DOUBLE",
        "mgnMode:SYMBOL", "mgnRatio:DOUBLE", "mmr:DOUBLE",
        "notionalUsd:DOUBLE", "optVal:DOUBLE", "pTime:TIMESTAMP",
        "pos:DOUBLE", "posCcy:SYMBOL", "posId:LONG", "posSide:SYMBOL",
        "thetaBS:DOUBLE", "thetaPA:DOUBLE", "tradeId:LONG",
        "uTime:TIMESTAMP", "upl:DOUBLE", "uplRatio:DOUBLE",
        "vegaBS:DOUBLE", "vegaPA:DOUBLE",
    ),
    key_columns=("instId", "posSide", "posId"),
    time_column="pTime",
    capacity=5000,
    latest=True,
    description="账户爆仓风险预警",
)

ACCOUNT_GREEKS = TableSpec(
    "accountGreeksStreamTable",
    cols(
        "instType:SYMBOL", "instId:SYMBOL", "ccy:SYMBOL",
        "delta:DOUBLE", "gamma:DOUBLE", "vega:DOUBLE", "theta:DOUBLE",
        "deltaBS:DOUBLE", "gammaBS:DOUBLE", "thetaBS:DOUBLE",
        "vegaBS:DOUBLE", "ts:TIMESTAMP",
    ),
    capacity=50_000,
    retention_minutes=1440,
    description="账户 Greeks",
)

SHARE_ACCOUNT_GREEKS = TableSpec(
    "shareAccountGreeksTable",
    ACCOUNT_GREEKS.columns,
    key_columns=("ccy",),
    time_column="ts",
    capacity=1000,
    latest=True,
    description="最新账户 Greeks",
)


# 表名 -> TableSpec 注册表
STREAM_TABLES: dict[str, TableSpec] = {
    spec.name: spec
    for spec in (
        TICK, SHARE_TICK,
        CANDLE, CANDLE_CONFIRM, SHARE_CANDLE,
        OPEN_INTEREST, FUNDING, MARK_PRICE, MARK_PRICE_CANDLE,
        INDEX_TICK, INDEX_CANDLE, PRICE_LIMIT, OPT_SUMMARY,
        ESTIMATED_PRICE, OPTION_TRADE, LIQUIDATION_ORDER,
        PRODUCT, STATUS,
        TRADE, BOOK,
        CANDLE_INDICATOR, SHARE_CANDLE_INDICATOR,
        TICK_INDICATOR, SHARE_TICK_INDICATOR,
        ORDER, SHARE_ORDER,
        ALGO_ORDER, SHARE_ALGO_ORDER, FILLS,
        ACCOUNT, POSITION, BALANCE, WARNING,
        ACCOUNT_GREEKS, SHARE_ACCOUNT_GREEKS,
    )
}

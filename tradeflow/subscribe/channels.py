"""OKX WebSocket 频道清单：订阅什么、走哪个端点、写哪些表。

本模块是频道参数的唯一来源；``Kernel`` 不再手写频道列表。
端点取值：
- ``public``: /ws/v5/public
- ``business``: /ws/v5/business（K 线等大流量频道）
- ``private``: /ws/v5/private（需登录）
- ``business-private``: /ws/v5/business 且需登录（策略委托）
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class ChannelSpec:
    name: str
    endpoint: str
    tables: tuple[str, ...]
    arg: str = "instId"      # instId / instType / instFamily / indexId / ccy / none
    auth: bool = False       # 是否需要登录
    default: bool = False    # 是否默认开启


CHANNEL_SPECS = {
    # ---------- 公共行情 ----------
    "tickers": ChannelSpec("tickers", "public", ("tickStreamTable",), default=True),
    "trades": ChannelSpec("trades", "public", ("tradeStreamTable",), default=True),
    "books5": ChannelSpec("books5", "public", ("bookStreamTable",)),
    "bbo-tbt": ChannelSpec("bbo-tbt", "public", ("bookStreamTable",)),
    "books": ChannelSpec("books", "public", ("bookStreamTable",)),
    "open-interest": ChannelSpec("open-interest", "public", ("openinterestStreamTable",), default=True),
    "funding-rate": ChannelSpec("funding-rate", "public", ("fundingrateStreamTable",), default=True),
    "mark-price": ChannelSpec("mark-price", "public", ("markpriceStreamTable",), default=True),
    "index-tickers": ChannelSpec("index-tickers", "public", ("indextickStreamTable",), arg="indexId", default=True),
    "price-limit": ChannelSpec("price-limit", "public", ("pricelimitStreamTable",)),
    "opt-summary": ChannelSpec("opt-summary", "public", ("optsummaryStreamTable",), arg="instFamily"),
    "estimated-price": ChannelSpec("estimated-price", "public", ("estimatedpriceStreamTable",), arg="instType"),
    "option-trades": ChannelSpec("option-trades", "public", ("optiontradeStreamTable",), arg="instFamily"),
    "liquidation-orders": ChannelSpec("liquidation-orders", "public", ("liquidationorderStreamTable",), arg="instType", default=True),
    "status": ChannelSpec("status", "public", ("statusStreamTable",), arg="none", default=True),
    "instruments": ChannelSpec("instruments", "public", ("productStreamTable",), arg="instType", default=True),
    # ---------- 业务端点（K 线等） ----------
    "candle1m": ChannelSpec("candle1m", "business", ("candleStreamTable", "candleConfirmStreamTable"), default=True),
    "candle5m": ChannelSpec("candle5m", "business", ("candleStreamTable", "candleConfirmStreamTable"), default=True),
    "mark-price-candle": ChannelSpec("mark-price-candle", "business", ("markpricecandleStreamTable",)),
    "index-candle": ChannelSpec("index-candle", "business", ("indexcandleStreamTable",), arg="indexId"),
    "trades-all": ChannelSpec("trades-all", "business", ("tradeStreamTable",)),
    # ---------- 私有（需登录） ----------
    "account": ChannelSpec("account", "private", ("shareAccountTable",), arg="none", auth=True, default=True),
    "positions": ChannelSpec("positions", "private", ("sharePositionTable",), arg="instType", auth=True, default=True),
    "orders": ChannelSpec("orders", "private", ("orderStreamTable", "shareOrderTable"), arg="instType", auth=True, default=True),
    "fills": ChannelSpec("fills", "private", ("fillsStreamTable",), arg="instType", auth=True),
    "balance_and_position": ChannelSpec("balance_and_position", "private", ("shareBalanceTable",), arg="none", auth=True, default=True),
    "liquidation-warning": ChannelSpec("liquidation-warning", "private", ("shareWarningTable",), arg="instType", auth=True, default=True),
    "account-greeks": ChannelSpec("account-greeks", "private", ("accountGreeksStreamTable", "shareAccountGreeksTable"), arg="ccy", auth=True, default=True),
    # ---------- 策略委托（business-private，需登录） ----------
    "orders-algo": ChannelSpec("orders-algo", "business-private", ("algoOrderStreamTable", "shareAlgoOrderTable"), arg="instType", auth=True),
    "algo-advance": ChannelSpec("algo-advance", "business-private", ("algoOrderStreamTable", "shareAlgoOrderTable"), arg="instType", auth=True),
}

# 默认频道组合
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
DEFAULT_ALGO_CHANNELS = ("orders-algo", "algo-advance")
OPTIONS_CHANNELS = ("opt-summary", "option-trades", "estimated-price")


def to_index_id(inst_id: str) -> str:
    """BTC-USDT-SWAP -> BTC-USDT（指数频道用的是指数 ID）。"""
    parts = inst_id.split("-")
    return "-".join(parts[:2]) if len(parts) >= 2 else inst_id


def to_inst_family(inst_id: str) -> str | None:
    """BTC-USDT-SWAP -> BTC-USDT（期权品种族）；非合约返回 None。"""
    parts = inst_id.split("-")
    if len(parts) >= 2 and parts[-1] in ("SWAP", "FUTURES", "OPTION"):
        return "-".join(parts[:2])
    return None


def get_channel_spec(name: str) -> ChannelSpec | None:
    if name.startswith("mark-price-candle"):
        return CHANNEL_SPECS["mark-price-candle"]
    if name.startswith("index-candle"):
        return CHANNEL_SPECS["index-candle"]
    if name.startswith("candle"):
        return CHANNEL_SPECS["candle1m"]
    return CHANNEL_SPECS.get(name)


def tables_for(channels) -> set[str]:
    """频道集合 -> 需要提前创建的表集合。"""
    tables = set()
    for name in channels:
        spec = get_channel_spec(name)
        if spec:
            tables.update(spec.tables)
    return tables

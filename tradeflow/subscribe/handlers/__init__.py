"""订阅数据解析分发器：channel -> handler -> list[Batch]。

handler 是纯函数，任何单条脏数据都不能拖垮连接，因此 dispatch 兜底异常。
"""
from dataclasses import dataclass  # noqa: F401  (Batch 从 batch.py 导出)

import pandas as pd  # noqa: F401

from ...utils.log import get_logger
from .batch import Batch
from .market import (
    parse_books, parse_candle, parse_estimated_price, parse_funding_rate,
    parse_index_candle, parse_index_tickers, parse_instruments,
    parse_liquidation_orders, parse_mark_price, parse_mark_price_candle,
    parse_open_interest, parse_option_trades, parse_opt_summary,
    parse_price_limit, parse_status, parse_tickers, parse_trades,
)

logger = get_logger()


def channel_key(channel: str) -> str:
    """candle1m/candle5m/mark-price-candle1m 等归一到 handler 注册键。"""
    if channel.startswith("mark-price-candle"):
        return "mark-price-candle"
    if channel.startswith("index-candle"):
        return "index-candle"
    if channel.startswith("candle"):
        return "candle"
    return channel


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


def dispatch(packet: dict) -> list[Batch]:
    """按频道分发；未注册频道告警忽略，解析异常丢弃本条并告警。"""
    arg = packet.get("arg") or {}
    channel = arg.get("channel", "")
    if not channel:
        return []
    handler = HANDLERS.get(channel_key(channel))
    if handler is None:
        logger.warning(f"未实现的频道，已忽略: {channel}")
        return []
    try:
        return [
            b for b in handler(packet)
            if b.frame is not None and not b.frame.empty
        ]
    except Exception as exc:
        logger.exception(f"解析频道 {channel} 失败，已丢弃本条: {exc}")
        return []

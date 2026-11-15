"""公共行情连接：/ws/v5/public 与 /ws/v5/business。"""
from ..setting import (
    REAL_CANDLE_URL, REAL_PUBLIC_URL,
    SIMULATE_CANDLE_URL, SIMULATE_PUBLIC_URL,
)
from .base import Subscribe


class PublicSubscribe(Subscribe):
    """公共行情（tickers/trades/盘口/资金费率/标记价格/指数/爆仓/状态/产品）。"""

    endpoint = "public"

    def default_url(self) -> str:
        return REAL_PUBLIC_URL if self.flag == "0" else SIMULATE_PUBLIC_URL


class CandleSubscribe(Subscribe):
    """业务端点行情（K 线、标记价格 K 线、指数 K 线、全量成交）。"""

    endpoint = "business"

    def default_url(self) -> str:
        return REAL_CANDLE_URL if self.flag == "0" else SIMULATE_CANDLE_URL

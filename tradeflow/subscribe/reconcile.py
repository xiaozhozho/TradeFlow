"""断线重连后的 REST 对账与 K 线缺口补齐。

原则：
- 私有连接重连后先对账（订单/持仓/账户快照覆盖），再让策略继续交易；
- 业务端点重连后补齐 K 线缺口；只插入缺口内的 bar，
  避免 REST 返回的重复 bar 污染指标引擎状态；
- 对账只做快照覆盖，不重放历史事件；订单历史以
  ``orderStreamTable`` + REST ``get_orders_history`` 为准。
"""
from __future__ import annotations

import pandas as pd

from ..stream.schema import STREAM_TABLES
from ..utils.log import get_logger

logger = get_logger()

_CANDLE_COLUMNS = ["ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "confirm"]


class Reconciler:
    """断线对账器：按端点选择对账内容。"""

    def __init__(self, kernel):
        self.kernel = kernel

    def after_reconnect(self, endpoint: str) -> None:
        try:
            if endpoint in ("private", "business-private"):
                self.sync_orders()
                self.sync_positions()
                self.sync_account()
            if endpoint == "business":
                self.backfill_candles()
        except Exception as exc:
            logger.exception(f"对账失败（endpoint={endpoint}）: {exc}")

    # ---------- 私有对账 ----------

    def sync_orders(self) -> None:
        packet = self.kernel.api.trade.get_order_list(instType="ANY")
        if packet.get("code") != "0":
            logger.warning(f"对账 orders 失败: {packet.get('code')} {packet.get('msg')}")
            return
        rows = packet.get("data") or []
        if rows:
            frame = STREAM_TABLES["shareOrderTable"].normalize(pd.DataFrame(rows))
            self.kernel.dolphin.data2MemoryTable("shareOrderTable", frame)

    def sync_positions(self) -> None:
        packet = self.kernel.api.account.get_positions(instType="ANY")
        if packet.get("code") != "0":
            logger.warning(f"对账 positions 失败: {packet.get('code')} {packet.get('msg')}")
            return
        rows = packet.get("data") or []
        if rows:
            frame = STREAM_TABLES["sharePositionTable"].normalize(pd.DataFrame(rows))
            self.kernel.dolphin.data2MemoryTable("sharePositionTable", frame)

    def sync_account(self) -> None:
        packet = self.kernel.api.account.get_account_balance()
        if packet.get("code") != "0":
            logger.warning(f"对账 account 失败: {packet.get('code')} {packet.get('msg')}")
            return
        details = (packet.get("data") or [{}])[0].get("details") or []
        if details:
            frame = STREAM_TABLES["shareAccountTable"].normalize(pd.DataFrame(details))
            self.kernel.dolphin.data2MemoryTable("shareAccountTable", frame)

    # ---------- K 线补齐 ----------

    def backfill_candles(self, bar: str = "1m", limit: int = 300) -> None:
        for instId in self.kernel.instIds:
            last = self.kernel.dolphin.runScript(
                f"exec max(ts) from candleStreamTable where instId='{instId}'"
            )
            if last is None:
                # 流表无历史：无需补缺口，订阅恢复后自然续上
                continue
            packet = self.kernel.api.market.get_history_candlesticks(
                instId, after="", before="", bar=bar, limit=str(limit),
            )
            if packet.get("code") != "0":
                logger.warning(f"补齐 {instId} K 线失败: {packet.get('code')}")
                continue
            rows = packet.get("data") or []
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=_CANDLE_COLUMNS)
            # REST 返回倒序（新到旧），只保留缺口之后的 bar
            df["_ts_ms"] = pd.to_numeric(df["ts"], errors="coerce")
            df = df[df["_ts_ms"] > int(last)].sort_values("_ts_ms")
            df = df.drop(columns=["_ts_ms"]).reset_index(drop=True)
            if df.empty:
                continue
            df["instId"] = instId
            df["channel"] = "candle1m"
            frame = STREAM_TABLES["candleStreamTable"].normalize(df)
            self.kernel.dolphin.data2MemoryTable("candleStreamTable", frame)
            logger.info(f"补齐 {instId} 缺口 K 线 {len(frame)} 根")

"""公共行情频道解析：packet -> Batch(table, DataFrame) 纯函数。

只做解析，不做 I/O；列对齐与类型转换统一由 TableSpec.normalize 完成。
"""
import pandas as pd

from ...utils.log import get_logger
from .batch import Batch

logger = get_logger()


def _data(packet):
    return packet.get("data") or []


def parse_tickers(packet):
    return [Batch("tickStreamTable", pd.DataFrame(_data(packet)))]


def parse_candle(packet):
    """K 线：同时产出全量批次与 confirm=1 批次（后者喂指标引擎）。"""
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
    """盘口长表。

    - books5 / bbo-tbt: 无状态展开（快照型推送）；
    - books: 增量盘口，经 OrderBook 序号校验，断裂时丢弃该条。
    """
    if source == "books":
        return _parse_books_incremental(packet)
    rows = []
    for item in _data(packet):
        for side, key in (("ask", "asks"), ("bid", "bids")):
            for level, level_data in enumerate(item.get(key) or [], start=1):
                rows.append({
                    "instId": item.get("instId"),
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


def _parse_books_incremental(packet):
    """books 频道：snapshot 重建 / update 增量，序号断裂丢弃。"""
    from ..orderbook import ORDER_BOOK

    action = packet.get("action", "snapshot")
    rows = []
    for item in _data(packet):
        inst_id = item.get("instId")
        seq_id = item.get("seqId", 0)
        prev_seq_id = item.get("prevSeqId", 0)
        if action == "snapshot":
            ORDER_BOOK.apply_snapshot(
                inst_id, item.get("bids") or [], item.get("asks") or [], seq_id=seq_id,
            )
        else:
            ok = ORDER_BOOK.apply_delta(
                inst_id, item.get("bids") or [], item.get("asks") or [],
                prev_seq_id=prev_seq_id, seq_id=seq_id,
            )
            if not ok:
                logger.warning(f"盘口序号断裂，丢弃本条: {inst_id} prevSeqId={prev_seq_id}")
                continue
        for side, key in (("ask", "asks"), ("bid", "bids")):
            for level, level_data in enumerate(item.get(key) or [], start=1):
                rows.append({
                    "instId": inst_id,
                    "ts": item.get("ts"),
                    "seqId": seq_id,
                    "prevSeqId": prev_seq_id,
                    "checksum": item.get("checksum", 0),
                    "side": side,
                    "level": level,
                    "px": level_data[0],
                    "sz": level_data[1],
                    "source": "books",
                })
    if not rows:
        return []
    return [Batch("bookStreamTable", pd.DataFrame(rows))]


def parse_liquidation_orders(packet):
    """爆仓单：遍历 data[*].details[*]，每笔明细一行。"""
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


def parse_open_interest(packet):
    return [Batch("openinterestStreamTable", pd.DataFrame(_data(packet)))]


def parse_funding_rate(packet):
    return [Batch("fundingrateStreamTable", pd.DataFrame(_data(packet)))]


def parse_mark_price(packet):
    return [Batch("markpriceStreamTable", pd.DataFrame(_data(packet)))]


def parse_mark_price_candle(packet):
    """标记价格 K 线：列顺序固定 [channel, instId, ts, o, h, l, c, confirm]。"""
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
    """指数 K 线：列顺序 [ts, o, h, l, c, confirm, instId, channel]。"""
    rows = []
    for item in _data(packet):
        ts, o, h, l, c, confirm = item[:6]
        rows.append({
            "ts": ts, "o": o, "h": h, "l": l, "c": c, "confirm": confirm,
            "instId": packet["arg"]["instId"],
            "channel": packet["arg"]["channel"],
        })
    return [Batch("indexcandleStreamTable", pd.DataFrame(rows))]


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
    """产品规格：真正落库 productStreamTable（旧实现只转换不写库）。"""
    return [Batch("productStreamTable", pd.DataFrame(_data(packet)))]

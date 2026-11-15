"""增量盘口维护与序号校验（books 频道，P1）。

OKX books 频道推送分 ``snapshot`` 与 ``update`` 两类：
- snapshot: 全量重建本地盘口；
- update: 依赖 ``prevSeqId == 上一条 seqId`` 的增量；
  序号断裂时丢弃该条并告警，等待下一次快照（或 REST 重建）。
"""
from __future__ import annotations

from ..utils.log import get_logger

logger = get_logger()


class OrderBook:
    """单实例维护多品种盘口：{instId: {"bids": {px: sz}, "asks": {px: sz}, "seq_id": int}}。"""

    def __init__(self):
        self._books: dict[str, dict] = {}

    def apply_snapshot(self, inst_id, bids, asks, seq_id) -> None:
        self._books[inst_id] = {
            "bids": {float(px): float(sz) for px, sz in bids if float(sz) > 0},
            "asks": {float(px): float(sz) for px, sz in asks if float(sz) > 0},
            "seq_id": int(seq_id),
        }

    def apply_delta(self, inst_id, bids, asks, prev_seq_id, seq_id) -> bool:
        """应用增量；prevSeqId 不连续返回 False 且不修改状态。"""
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
        book = self._books.get(inst_id)
        if not book or not book["bids"]:
            return None
        return max(book["bids"])

    def best_ask(self, inst_id):
        book = self._books.get(inst_id)
        if not book or not book["asks"]:
            return None
        return min(book["asks"])

    def level_count(self, inst_id, side: str) -> int:
        book = self._books.get(inst_id)
        if not book:
            return 0
        return len(book[side])

    def state(self, inst_id) -> dict | None:
        return self._books.get(inst_id)


# books 频道的共享盘口状态（handlers 内有状态是刻意的：增量依赖连续序号）
ORDER_BOOK = OrderBook()

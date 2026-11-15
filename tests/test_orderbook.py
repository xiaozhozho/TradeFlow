"""增量盘口 OrderBook 与序号校验测试。"""
from tradeflow.subscribe.orderbook import OrderBook


def test_apply_snapshot_and_delta():
    book = OrderBook()
    book.apply_snapshot("BTC", [[100.0, 1.0], [99.0, 2.0]], [[101.0, 1.0]], seq_id=10)
    assert book.best_bid("BTC") == 100.0
    assert book.best_ask("BTC") == 101.0
    # 100 档被删（sz=0）后，最优买价变为 99
    book.apply_delta("BTC", [[100.0, 0.0]], [], prev_seq_id=10, seq_id=11)
    assert book.best_bid("BTC") == 99.0


def test_gap_detected():
    book = OrderBook()
    book.apply_snapshot("BTC", [[100.0, 1.0]], [[101.0, 1.0]], seq_id=10)
    assert book.apply_delta("BTC", [], [], prev_seq_id=12, seq_id=13) is False
    # 断裂后仍可由快照恢复
    book.apply_snapshot("BTC", [[98.0, 1.0]], [[103.0, 1.0]], seq_id=13)
    assert book.best_bid("BTC") == 98.0


def test_delta_without_snapshot_rejected():
    book = OrderBook()
    assert book.apply_delta("BTC", [[100.0, 1.0]], [], prev_seq_id=1, seq_id=2) is False


def test_zero_size_removes_only_that_level():
    book = OrderBook()
    book.apply_snapshot("BTC", [[100.0, 1.0], [99.0, 1.0]], [], seq_id=1)
    book.apply_delta("BTC", [[99.0, 0.0]], [], prev_seq_id=1, seq_id=2)
    assert book.best_bid("BTC") == 100.0
    assert book.level_count("BTC", "bids") == 1


def test_books_dispatch_gap_drops_rows():
    from tradeflow.subscribe.handlers import dispatch
    from tradeflow.subscribe.orderbook import ORDER_BOOK

    snapshot = {
        "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
        "action": "snapshot",
        "data": [{
            "instId": "BTC-USDT-SWAP", "seqId": 10,
            "asks": [["101", "1"]], "bids": [["100", "1"]],
        }],
    }
    batches = dispatch(snapshot)
    assert batches and batches[0].table == "bookStreamTable"
    assert len(batches[0].frame) == 2

    # 正常增量：prevSeqId=10 -> seqId=11
    delta = {
        "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
        "action": "update",
        "data": [{
            "instId": "BTC-USDT-SWAP", "prevSeqId": 10, "seqId": 11,
            "asks": [["101.5", "2"]], "bids": [],
        }],
    }
    batches = dispatch(delta)
    assert batches and len(batches[0].frame) == 1

    # 序号断裂：prevSeqId=13 != 11 -> 丢弃，无批次
    gap = {
        "arg": {"channel": "books", "instId": "BTC-USDT-SWAP"},
        "action": "update",
        "data": [{
            "instId": "BTC-USDT-SWAP", "prevSeqId": 13, "seqId": 14,
            "asks": [["102", "1"]], "bids": [],
        }],
    }
    assert dispatch(gap) == []
    # 盘口内存状态未受污染
    assert ORDER_BOOK.best_bid("BTC-USDT-SWAP") == 100.0

"""公共行情频道解析测试。"""
from tradeflow.subscribe.handlers import Batch, channel_key, dispatch


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
    assert isinstance(batches[0], Batch)
    assert batches[0].table == "tickStreamTable"
    assert len(batches[0].frame) == 1


def test_candle_writes_all_and_confirmed():
    packet = {
        "arg": {"channel": "candle1m", "instId": "BTC-USDT-SWAP"},
        "data": [["1704067200000", "1", "2", "0.5", "1.5", "10", "10", "15", "0"]],
    }
    batches = dispatch(packet)
    tables = {b.table for b in batches}
    assert tables == {"candleStreamTable"}  # 未收盘只进全量表


def test_candle_confirmed_goes_to_both_tables():
    packet = {
        "arg": {"channel": "candle1m", "instId": "BTC-USDT-SWAP"},
        "data": [["1704067200000", "1", "2", "0.5", "1.5", "10", "10", "15", "1"]],
    }
    tables = {b.table for b in dispatch(packet)}
    assert tables == {"candleStreamTable", "candleConfirmStreamTable"}


def test_trades_fills_count_and_source():
    packet = {
        "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "instId": "BTC-USDT-SWAP", "tradeId": "123", "px": "100",
            "sz": "1", "side": "buy", "ts": "1704067200000",
        }],
    }
    batches = dispatch(packet)
    assert batches[0].table == "tradeStreamTable"
    assert batches[0].frame.loc[0, "count"] == 1
    assert batches[0].frame.loc[0, "source"] == "trades"


def test_books5_expands_levels():
    packet = {
        "arg": {"channel": "books5", "instId": "BTC-USDT-SWAP"},
        "data": [{
            "instId": "BTC-USDT-SWAP",
            "asks": [["101", "1"], ["102", "2"]],
            "bids": [["99", "3"]],
            "ts": "1704067200000",
        }],
    }
    batches = dispatch(packet)
    assert batches[0].table == "bookStreamTable"
    assert len(batches[0].frame) == 3
    assert set(batches[0].frame["side"]) == {"ask", "bid"}


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
    assert len(batches[0].frame) == 2  # 旧实现只取 details[0]


def test_unknown_channel_is_ignored():
    assert dispatch({"arg": {"channel": "no-such"}, "data": []}) == []


def test_empty_data_is_skipped():
    assert dispatch({"arg": {"channel": "tickers", "instId": "X"}, "data": []}) == []


def test_dirty_data_does_not_raise():
    packet = {"arg": {"channel": "tickers"}, "data": [{"ts": "bad", "instId": None}]}
    batches = dispatch(packet)  # dispatch 不抛异常
    assert isinstance(batches, list)


def test_channel_key():
    assert channel_key("candle1m") == "candle"
    assert channel_key("candle5m") == "candle"
    assert channel_key("mark-price-candle1D") == "mark-price-candle"
    assert channel_key("index-candle1m") == "index-candle"
    assert channel_key("tickers") == "tickers"

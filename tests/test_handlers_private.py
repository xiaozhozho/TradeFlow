"""私有频道解析测试。"""
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


def test_positions():
    packet = {
        "arg": {"channel": "positions", "instType": "ANY"},
        "data": [{
            "instId": "BTC-USDT-SWAP", "posSide": "long", "pos": "1",
            "pTime": "1704067200000", "posId": "9",
        }],
    }
    batches = dispatch(packet)
    assert batches[0].table == "sharePositionTable"


def test_fills():
    packet = {
        "arg": {"channel": "fills", "instType": "ANY"},
        "data": [{
            "instId": "BTC-USDT-SWAP", "fillPx": "100", "fillSz": "1",
            "ts": "1704067200000",
        }],
    }
    assert dispatch(packet)[0].table == "fillsStreamTable"


def test_balance_uses_baldata():
    packet = {
        "arg": {"channel": "balance_and_position"},
        "data": [{"balData": [{"ccy": "USDT", "cashBal": "1", "uTime": "1704067200000"}]}],
    }
    batches = dispatch(packet)
    assert batches[0].table == "shareBalanceTable"
    assert batches[0].frame.loc[0, "ccy"] == "USDT"


def test_liquidation_warning():
    packet = {
        "arg": {"channel": "liquidation-warning", "instType": "ANY"},
        "data": [{"instId": "BTC-USDT-SWAP", "posSide": "long", "posId": "1"}],
    }
    assert dispatch(packet)[0].table == "shareWarningTable"


def test_account_greeks_writes_stream_and_snapshot():
    packet = {
        "arg": {"channel": "account-greeks", "ccy": "USDT"},
        "data": [{"ccy": "USDT", "delta": "0.1", "ts": "1704067200000"}],
    }
    tables = {b.table for b in dispatch(packet)}
    assert tables == {"accountGreeksStreamTable", "shareAccountGreeksTable"}


def test_algo_list_fields():
    packet = {
        "arg": {"channel": "orders-algo", "instType": "ANY"},
        "data": [{
            "instId": "BTC-USDT-SWAP", "algoId": "9",
            "ordIdList": ["1", "2"], "subAlgoIdList": ["3"],
            "cTime": "1704067200000", "uTime": "1704067200000",
        }],
    }
    batches = dispatch(packet)
    tables = {b.table for b in batches}
    assert tables == {"algoOrderStreamTable", "shareAlgoOrderTable"}
    assert batches[0].frame.loc[0, "ordIdList"] == "1,2"
    assert batches[0].frame.loc[0, "subAlgoIdList"] == "3"
    # 原始报文保留在 raw 列
    assert '"algoId":"9"' in batches[0].frame.loc[0, "raw"]


def test_algo_advance_uses_same_handler():
    packet = {
        "arg": {"channel": "algo-advance", "instType": "ANY"},
        "data": [{"instId": "BTC-USDT-SWAP", "algoId": "7"}],
    }
    assert dispatch(packet)[0].table == "algoOrderStreamTable"

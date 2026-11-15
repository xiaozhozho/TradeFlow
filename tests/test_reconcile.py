"""断线对账与 K 线补齐测试。"""
import pandas as pd

from tradeflow.subscribe.reconcile import Reconciler


class FakeAPI:
    def __init__(self, candles=None):
        self.trade = self
        self.account = self
        self.market = self
        self.calls = []
        self._candles = candles or []

    def get_order_list(self, **kwargs):
        self.calls.append(("orders", kwargs))
        return {"code": "0", "data": [
            {"ordId": "1", "instId": "BTC-USDT-SWAP", "state": "live",
             "cTime": "1704067200000", "uTime": "1704067200000"},
        ]}

    def get_positions(self, **kwargs):
        self.calls.append(("positions", kwargs))
        return {"code": "0", "data": [
            {"instId": "BTC-USDT-SWAP", "posSide": "net", "pos": "1",
             "pTime": "1704067200000"},
        ]}

    def get_account_balance(self, **kwargs):
        self.calls.append(("account", kwargs))
        return {"code": "0", "data": [{"details": [
            {"ccy": "USDT", "eq": "100", "uTime": "1704067200000"},
        ]}]}

    def get_history_candlesticks(self, instId, after="", before="", bar="", limit=""):
        self.calls.append(("candles", {"instId": instId, "bar": bar, "limit": limit}))
        return {"code": "0", "data": self._candles}


class FakeDolphin:
    def __init__(self, max_ts=None):
        self._max_ts = max_ts
        self.written = []
        self.scripts = []

    def runScript(self, script, *data):
        self.scripts.append(script)
        if "exec max(ts)" in str(script):
            return self._max_ts
        return None

    def data2MemoryTable(self, tableName, *data):
        self.written.append((tableName, data[0] if data else None))


class FakeKernel:
    def __init__(self, candles=None, max_ts=None):
        self.api = FakeAPI(candles)
        self.dolphin = FakeDolphin(max_ts)
        self.instIds = {"BTC-USDT-SWAP"}


def test_private_reconnect_syncs_three_sources():
    kernel = FakeKernel()
    Reconciler(kernel).after_reconnect("private")
    assert {call[0] for call in kernel.api.calls} == {"orders", "positions", "account"}
    tables = [table for table, _ in kernel.dolphin.written]
    assert tables == ["shareOrderTable", "sharePositionTable", "shareAccountTable"]


def test_business_private_reconnect_syncs_too():
    kernel = FakeKernel()
    Reconciler(kernel).after_reconnect("business-private")
    assert {call[0] for call in kernel.api.calls} == {"orders", "positions", "account"}


def test_public_reconnect_does_nothing():
    kernel = FakeKernel()
    Reconciler(kernel).after_reconnect("public")
    assert kernel.api.calls == []
    assert kernel.dolphin.written == []


def test_backfill_candles_only_fills_gap():
    """补齐只插入 last 之后的 bar，避免重复 bar 污染指标引擎状态。"""
    candles = [
        ["1704067140000", "1", "2", "0.5", "1.5", "10", "10", "15", "1"],  # 早于 last -> 丢弃
        ["1704067200000", "1", "2", "0.5", "1.5", "10", "10", "15", "1"],  # 早于 last -> 丢弃
        ["1704067260000", "1.5", "2.5", "1", "2", "11", "11", "16", "1"],  # 晚于 last -> 保留
    ]
    kernel = FakeKernel(candles=candles, max_ts=1704067200000)
    Reconciler(kernel).after_reconnect("business")
    writes = [df for table, df in kernel.dolphin.written if table == "candleStreamTable"]
    assert len(writes) == 1
    assert len(writes[0]) == 1
    assert writes[0].loc[0, "ts"] == pd.Timestamp("2024-01-01 00:01:00")  # 1704067260000
    assert writes[0].loc[0, "channel"] == "candle1m"


def test_backfill_skips_when_no_history():
    kernel = FakeKernel(candles=[["1704067260000", "1", "2", "0.5", "1.5", "10", "10", "15", "1"]],
                        max_ts=None)
    Reconciler(kernel).after_reconnect("business")
    assert not [t for t, _ in kernel.dolphin.written if t == "candleStreamTable"]


def test_sync_skips_on_error_code():
    kernel = FakeKernel()
    kernel.api.calls = []
    orig = kernel.api.get_order_list

    def fail(**kwargs):
        kernel.api.calls.append(("orders", kwargs))
        return {"code": "50011", "data": []}

    kernel.api.get_order_list = fail
    Reconciler(kernel).after_reconnect("private")
    tables = [table for table, _ in kernel.dolphin.written]
    assert "shareOrderTable" not in tables
    assert ("positions", {}) in kernel.api.calls or any(c[0] == "positions" for c in kernel.api.calls)
    orig()

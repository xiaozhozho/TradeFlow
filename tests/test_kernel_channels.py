"""Kernel 频道参数生成测试（mock Dolphin，不连库、不联网）。"""
import logging
from types import SimpleNamespace

import pytest

from tradeflow.kernel import Kernel


class FakeDolphin:
    def __init__(self, *args, **kwargs):
        self.prepared_tables = []
        self.recreate_flags = []

    def prepare_streams(self, table_names, recreate=False):
        self.prepared_tables.extend(table_names)
        self.recreate_flags.append(recreate)


@pytest.fixture
def kernel(monkeypatch):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    return Kernel(flag="1", private=False, instIds={"BTC-USDT-SWAP"})


def test_default_public_channels(kernel):
    names = {item["channel"] for item in kernel.publicChannels}
    assert {"tickers", "trades", "funding-rate", "mark-price", "index-tickers",
            "open-interest", "liquidation-orders", "status", "instruments"} <= names


def test_default_market_channels_has_no_depth(kernel):
    names = {item["channel"] for item in kernel.publicChannels}
    assert "books5" not in names
    assert "books" not in names


def test_depth_toggle(monkeypatch):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    kernel = Kernel(flag="1", private=False, instIds={"BTC-USDT-SWAP"}, depth="books5")
    names = {item["channel"] for item in kernel.publicChannels}
    assert "books5" in names


def test_candle_channels_default(kernel):
    names = {item["channel"] for item in kernel.candleChannels}
    assert names == {"candle1m", "candle5m"}
    for item in kernel.candleChannels:
        assert item["instId"] in kernel.instIds


def test_index_tickers_converts_instid(kernel):
    args = [a for a in kernel.publicChannels if a["channel"] == "index-tickers"]
    assert args and args[0]["instId"] == "BTC-USDT"


def test_public_args_per_channel(kernel):
    channels = {(item["channel"],): item for item in kernel.publicChannels}
    # status 无参数
    status = [a for a in kernel.publicChannels if a["channel"] == "status"]
    assert status == [{"channel": "status"}]
    # instruments / liquidation-orders 带 instType
    inst = [a for a in kernel.publicChannels if a["channel"] == "instruments"]
    assert inst == [{"channel": "instruments", "instType": "SWAP"}]
    liq = [a for a in kernel.publicChannels if a["channel"] == "liquidation-orders"]
    assert liq == [{"channel": "liquidation-orders", "instType": "SWAP"}]
    # 普通频道按 instId 展开
    tick = [a for a in kernel.publicChannels if a["channel"] == "tickers"]
    assert tick == [{"channel": "tickers", "instId": "BTC-USDT-SWAP"}]


def test_options_toggle(monkeypatch):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    kernel = Kernel(
        flag="1", private=False, instIds={"BTC-USDT-SWAP"}, options=True,
    )
    names = {item["channel"] for item in kernel.publicChannels}
    assert {"opt-summary", "option-trades"} <= names
    summary = [a for a in kernel.publicChannels if a["channel"] == "opt-summary"]
    assert summary[0]["instFamily"] == "BTC-USDT"


def test_private_channels(monkeypatch):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    kernel = Kernel(flag="1", private=True, instIds={"BTC-USDT-SWAP"})
    names = {item["channel"] for item in kernel.privateChannels}
    assert {"account", "positions", "orders", "liquidation-warning",
            "balance_and_position", "account-greeks"} <= names
    for item in kernel.privateChannels:
        if item["channel"] in ("positions", "orders", "liquidation-warning"):
            assert item["instType"] == "ANY"


def test_algo_channels(monkeypatch):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    kernel = Kernel(flag="1", private=False, algo=True, instIds={"BTC-USDT-SWAP"})
    names = {item["channel"] for item in kernel.algoChannels}
    assert names == {"orders-algo", "algo-advance"}


def test_enabled_channels_union(kernel):
    names = kernel.enabled_channels()
    assert "tickers" in names and "candle1m" in names
    assert "account" not in names  # private=False


def test_prepare_streams_required_tables(monkeypatch):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    kernel = Kernel(
        flag="1", private=False, instIds={"BTC-USDT-SWAP"},
        depth="books5", algo=True,
    )
    kernel.prepare()
    tables = set(kernel.dolphin.prepared_tables)
    # 核心表
    assert {"tickStreamTable", "shareTickTable", "candleStreamTable",
            "candleConfirmStreamTable", "orderStreamTable",
            "shareOrderTable"} <= tables
    # 频道表
    assert "bookStreamTable" in tables        # depth=books5
    assert "tradeStreamTable" in tables       # trades
    assert "fundingrateStreamTable" in tables
    assert "algoOrderStreamTable" in tables   # algo=True


# ---------------- RiskManage ----------------

from tradeflow.risk import RiskManage  # noqa: E402


class FakeTradeAPI:
    def __init__(self):
        self.calls = []

    def close_positions(self, instId, mgnMode, posSide, ccy=""):
        self.calls.append((instId, mgnMode, posSide, ccy))
        return {"code": "0"}


class FakeRiskKernel:
    def __init__(self, positions):
        self._positions = positions
        self.logger = logging.getLogger("test-risk")
        self.api = SimpleNamespace(trade=FakeTradeAPI())

    def getPositions(self):
        return [dict(p) for p in self._positions]

    def log(self, msg):
        pass


def _position(ratio="-0.5", pos="1"):
    return {"instId": "BTC-USDT-SWAP", "posSide": "net", "pos": pos,
            "uplRatio": ratio, "mgnMode": "cross", "ccy": "USDT", "lever": "30"}


def test_risk_close_on_max_loss():
    kernel = FakeRiskKernel([_position()])
    risk = RiskManage(kernel)
    risk.run()
    assert kernel.api.trade.calls == [("BTC-USDT-SWAP", "cross", "net", "USDT")]


def test_risk_no_duplicate_close_while_position_open():
    kernel = FakeRiskKernel([_position()])
    risk = RiskManage(kernel)
    risk.run()
    risk.run()
    risk.run()
    # 平仓请求发出后，仓位未消失前不重复发单
    assert len(kernel.api.trade.calls) == 1


def test_risk_resumes_after_position_closed():
    kernel = FakeRiskKernel([_position(pos="1")])
    risk = RiskManage(kernel)
    risk.run()
    kernel._positions = [_position(pos="0")]  # 仓位归零 -> 解除抑制
    risk.run()
    kernel._positions = [_position(pos="1")]  # 重新持仓 -> 允许再次触发
    risk.run()
    assert len(kernel.api.trade.calls) == 2


def test_risk_take_profit():
    kernel = FakeRiskKernel([_position(ratio="1000")])
    risk = RiskManage(kernel)
    risk.run()
    assert len(kernel.api.trade.calls) == 1


def test_risk_ignores_zero_position():
    kernel = FakeRiskKernel([_position(pos="0")])
    risk = RiskManage(kernel)
    risk.run()
    assert kernel.api.trade.calls == []

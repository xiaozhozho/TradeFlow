"""声明式指标测试：表达式生成 / 动态表与引擎注册 / 策略读取。"""
import pandas as pd
import pytest

from tradeflow.kernel import Kernel
from tradeflow.simulation import FakeApi, FakeDolphin
from tradeflow.strategy import Strategy
from tradeflow.stream import indicators as ind
from tradeflow.stream.engines import ENGINE_SPECS
from tradeflow.stream.schema import STREAM_TABLES


def test_declaration_to_metrics_basic():
    metrics = dict(ind.declaration_to_metrics({"ma": [5, 15], "rsi": [14]}))
    assert metrics["ma_5"] == "mavg(c, 5, 1)"
    assert metrics["ma_15"] == "mavg(c, 15, 1)"
    assert "prev(c)" in metrics["rsi_14"]


def test_declaration_boll_expands_three_columns():
    metrics = dict(ind.declaration_to_metrics({"boll": [(20, 2)]}))
    assert set(metrics) == {"boll_20_2_up", "boll_20_2_mid", "boll_20_2_dn"}
    assert "+ 2 * " in metrics["boll_20_2_up"]
    assert "- 2 * " in metrics["boll_20_2_dn"]


def test_declaration_rejects_unknown_indicator():
    with pytest.raises(ValueError):
        ind.declaration_to_metrics({"kdj": [9]})


def test_declaration_rejects_wrong_arity():
    with pytest.raises(ValueError):
        ind.declaration_to_metrics({"macd": [12]})


class RsiStrategy(Strategy):
    instIds = set()
    candles = ("candle1m",)
    indicators = {"ma": [5], "rsi": [14]}

    def preinit(self):
        pass

    def init(self):
        pass

    def unsubscribeTable(self):
        pass

    def dropStreamTable(self):
        pass

    def dropStreamEngine(self):
        pass

    def scan(self):
        pass


def test_kernel_prepare_registers_custom_indicator_tables_and_engines(monkeypatch):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    kernel = Kernel(flag="1", private=False, warmup=0, instIds=set(), api=FakeApi())
    kernel.addStrategy(RsiStrategy)
    kernel.prepare()
    tables = set(kernel.dolphin.prepared_tables)
    assert "indicator_RsiStrategyStreamTable" in tables
    assert "shareRsiStrategyIndicatorTable" in tables
    engines = set(kernel.dolphin.prepared_engines)
    assert "RsiStrategyIndicatorEngine" in engines
    assert "RsiStrategyIndicatorAction" in engines

    spec = ENGINE_SPECS["RsiStrategyIndicatorEngine"]
    assert spec.key_columns == ("instId", "channel")
    assert "rsi_14" in spec.metrics and "prev(c)" in spec.metrics
    action = ENGINE_SPECS["RsiStrategyIndicatorAction"]
    assert action.source == "indicator_RsiStrategyStreamTable"
    assert action.target == "shareRsiStrategyIndicatorTable"

    stream = STREAM_TABLES["indicator_RsiStrategyStreamTable"]
    assert stream.names[:3] == ["instId", "channel", "ts"]
    assert "ma_5" in stream.names
    snapshot = STREAM_TABLES["shareRsiStrategyIndicatorTable"]
    assert snapshot.latest is True
    assert snapshot.key_columns == ("instId", "channel")


def test_declared_strategy_reads_own_indicator_table():
    table = "indicator_RsiStrategyStreamTable"
    rows = pd.DataFrame([
        {"instId": "BTC-USDT-SWAP", "channel": "candle1m",
         "ts": pd.Timestamp("2026-01-01 00:00:00"), "ma_5": 1.0, "rsi_14": 50.0},
    ])
    kernel = Kernel(flag="1", private=False, warmup=0, instIds=set(),
                    dolphin=FakeDolphin({table: rows}), api=FakeApi())
    strategy = RsiStrategy(kernel)
    frame = strategy.getIndicators("candle1m")
    assert frame.iloc[-1]["rsi_14"] == 50.0


def test_strategy_without_declaration_uses_default_table():
    class PlainStrategy(Strategy):
        instIds = set()

        def preinit(self):
            pass

        def init(self):
            pass

        def unsubscribeTable(self):
            pass

        def dropStreamTable(self):
            pass

        def dropStreamEngine(self):
            pass

        def scan(self):
            pass

    rows = pd.DataFrame([
        {"instId": "BTC-USDT-SWAP", "channel": "candle1m",
         "ts": pd.Timestamp("2026-01-01 00:00:00"), "ma5": 1.0},
    ])
    kernel = Kernel(flag="1", private=False, warmup=0, instIds=set(),
                    dolphin=FakeDolphin({"candleIndicatorStreamTable": rows}), api=FakeApi())
    strategy = PlainStrategy(kernel)
    frame = strategy.getIndicators("candle1m")
    assert frame.iloc[-1]["ma5"] == 1.0

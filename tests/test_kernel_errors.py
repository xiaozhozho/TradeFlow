"""Kernel 异常隔离测试：策略/风控异常不得终止主循环。"""
import logging
from types import SimpleNamespace

import pytest

from tradeflow.kernel import Kernel
from tradeflow.strategy import Strategy


class FakeDolphin:
    def __init__(self, *args, **kwargs):
        pass


class FakeApi:
    def __init__(self, *args, **kwargs):
        self.account = SimpleNamespace(set_position_mode=lambda mode: {"code": "0"})
        self.trade = SimpleNamespace()


def make_kernel(monkeypatch, **kwargs):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    return Kernel(flag="1", private=False, instIds=set(), api=FakeApi(), **kwargs)


class RecordingStrategy(Strategy):
    instIds = set()

    def __init__(self, kernel):
        super().__init__(kernel)
        self.scans = 0

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
        self.scans += 1


class FailingStrategy(RecordingStrategy):
    def scan(self):
        self.scans += 1
        raise RuntimeError("boom")


class FlakyStrategy(RecordingStrategy):
    def __init__(self, kernel):
        super().__init__(kernel)
        self.fail = True

    def scan(self):
        self.scans += 1
        if self.fail:
            raise RuntimeError("boom")


def test_flag_defaults_to_simulated(monkeypatch):
    kernel = make_kernel(monkeypatch)
    assert kernel.flag == "1"


def test_live_flag_warns(monkeypatch, caplog):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    with caplog.at_level(logging.WARNING):
        Kernel(flag="0", private=False, instIds=set(), api=FakeApi())
    assert any("实盘" in record.message for record in caplog.records)


def test_strategy_exception_does_not_stop_other_strategies(monkeypatch):
    kernel = make_kernel(monkeypatch)
    bad = FailingStrategy(kernel)
    good = RecordingStrategy(kernel)
    kernel.strategies = [bad, good]
    kernel.run()
    assert bad.scans == 1
    assert good.scans == 1
    health = kernel.health()["strategies"]
    assert health["FailingStrategy#0"]["errors"] == 1
    assert health["RecordingStrategy#1"]["errors"] == 0
    assert health["FailingStrategy#0"]["disabled"] is False


def test_strategy_disabled_after_max_consecutive_errors(monkeypatch):
    kernel = make_kernel(monkeypatch, max_errors=3)
    bad = FailingStrategy(kernel)
    kernel.strategies = [bad]
    for _ in range(3):
        kernel.run()
    assert bad.disabled is True
    scans = bad.scans
    kernel.run()  # 停用后不再执行
    assert bad.scans == scans
    assert kernel.health()["strategies"]["FailingStrategy#0"]["disabled"] is True


def test_success_resets_consecutive_error_count(monkeypatch):
    kernel = make_kernel(monkeypatch, max_errors=2)
    strategy = FlakyStrategy(kernel)
    kernel.strategies = [strategy]
    kernel.run()          # 失败 -> 1
    strategy.fail = False
    kernel.run()          # 成功 -> 归零
    strategy.fail = True
    kernel.run()          # 失败 -> 1（若未归零则是 3，会被停用）
    assert strategy.disabled is False
    assert strategy._error_count == 1


def test_risk_exception_isolated(monkeypatch):
    kernel = make_kernel(monkeypatch)
    kernel.risk = SimpleNamespace(run=lambda: (_ for _ in ()).throw(RuntimeError("risk boom")))
    strategy = RecordingStrategy(kernel)
    kernel.strategies = [strategy]
    kernel.run()
    assert strategy.scans == 1
    assert kernel.health()["risk"]["errors"] == 1

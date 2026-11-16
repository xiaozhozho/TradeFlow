"""Trader 保证金换算 / 限额 / 批量 / 撤单测试（离线：FakeDolphin + FakeApi）。"""
import pandas as pd
import pytest

from tradeflow.kernel import Kernel
from tradeflow.simulation import FakeApi, FakeDolphin
from tradeflow.strategy import Strategy

INST_ID = "BTC-USDT-SWAP"


class DummyStrategy(Strategy):
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


def make_kernel(positions=None, **kwargs):
    tables = {
        "productStreamTable": pd.DataFrame([{
            "instId": INST_ID, "lotSz": 0.01, "minSz": 0.01,
            "tickSz": 0.1, "ctVal": 0.01,
        }]),
        "shareTickTable": pd.DataFrame([{"instId": INST_ID, "last": 100.0}]),
        "sharePositionTable": positions if positions is not None else pd.DataFrame(
            columns=["instId", "posSide", "pos", "mgnMode", "ccy", "lever"]),
        "shareOrderTable": pd.DataFrame(columns=["instId", "ordId", "state"]),
    }
    kernel = Kernel(flag="1", private=False, warmup=0, instIds=set(),
                    dolphin=FakeDolphin(tables), api=FakeApi(), **kwargs)
    kernel.risk.lever = 30
    return kernel


def test_size_by_margin_uses_tick_price():
    kernel = make_kernel()
    size = kernel.trader.sizeByMargin(INST_ID, 10)
    assert size == pytest.approx(300.0)  # 10 x 30 / (100 x 0.01)


def test_size_by_margin_explicit_price():
    kernel = make_kernel()
    size = kernel.trader.sizeByMargin(INST_ID, 10, price=200.0)
    assert size == pytest.approx(150.0)


def test_buy_by_margin_places_rounded_order():
    kernel = make_kernel()
    strategy = DummyStrategy(kernel)
    packet = strategy.buyByMargin(INST_ID, 10)
    assert packet["code"] == "0"
    order = kernel.api.trade.orders[0]
    assert order["side"] == "buy"
    assert float(order["sz"]) == 300.0


def test_max_order_margin_rejects_oversized_order():
    kernel = make_kernel(max_order_margin=5)
    strategy = DummyStrategy(kernel)
    with pytest.raises(ValueError):
        strategy.buyByMargin(INST_ID, 10)
    assert kernel.api.trade.orders == []


def test_max_position_margin_accumulates_existing_position():
    positions = pd.DataFrame([
        {"instId": INST_ID, "posSide": "net", "pos": "5",
         "mgnMode": "cross", "ccy": "USDT", "lever": "30"},
    ])
    # 现有 5 张占用保证金 = 5 x 0.01 x 100 / 30 ≈ 0.167U
    kernel = make_kernel(positions=positions, max_position_margin=10.1)
    strategy = DummyStrategy(kernel)
    with pytest.raises(ValueError):
        strategy.buyByMargin(INST_ID, 10)
    assert kernel.api.trade.orders == []
    # 上限放宽后可以下单
    kernel = make_kernel(positions=positions, max_position_margin=100)
    DummyStrategy(kernel).buyByMargin(INST_ID, 10)
    assert len(kernel.api.trade.orders) == 1


def test_batch_buy_splits_by_20():
    kernel = make_kernel()
    orders = [{"instId": INST_ID, "sz": 1} for _ in range(25)]
    packets = kernel.trader.batchBuy(orders)
    assert isinstance(packets, list) and len(packets) == 2
    batches = kernel.api.trade.batches
    assert [len(batch) for batch in batches] == [20, 5]
    assert batches[0][0]["side"] == "buy"
    assert float(batches[0][0]["sz"]) == 1.0


def test_cancel_order_passthrough():
    kernel = make_kernel()
    packet = kernel.trader.cancel(INST_ID, ordId="123")
    assert packet["code"] == "0"
    assert kernel.api.trade.cancels == [{"instId": INST_ID, "ordId": "123", "clOrdId": ""}]

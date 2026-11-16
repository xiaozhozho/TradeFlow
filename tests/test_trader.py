"""Trader 交易便捷层测试（离线：桩 DolphinDB / 桩 OKX API）。"""
from types import SimpleNamespace

import pandas as pd
import pytest

from tradeflow.kernel import Kernel
from tradeflow.trade import Trader


class StubDolphin:
    def __init__(self, product=None, positions=None, account=None):
        self.product = product
        self.positions = positions if positions is not None else pd.DataFrame()
        self.account = account

    def runScript(self, script, *args, **kwargs):
        if "productStreamTable" in script:
            if self.product is None:
                raise RuntimeError("no productStreamTable")
            return self.product
        if "sharePositionTable" in script:
            return self.positions
        if script.strip() == "shareAccountTable":
            return self.account
        raise NotImplementedError(script)


class StubPublic:
    def __init__(self, data=None):
        self.data = data or []
        self.calls = []

    def get_instruments(self, instType, uly="", instId="", instFamily=""):
        self.calls.append((instType, instId))
        return {"code": "0", "msg": "", "data": list(self.data)}


class StubTrade:
    def __init__(self):
        self.place_calls = []
        self.close_calls = []

    def place_order(self, **kwargs):
        self.place_calls.append(kwargs)
        return {"code": "0", "msg": "", "data": [{"ordId": "1", "clOrdId": kwargs.get("clOrdId", "")}]}

    def close_positions(self, instId, mgnMode, posSide="", ccy=""):
        self.close_calls.append(
            {"instId": instId, "mgnMode": mgnMode, "posSide": posSide, "ccy": ccy}
        )
        return {"code": "0", "msg": "", "data": []}


class StubKernel:
    def __init__(self, dolphin, public=None, trade=None):
        self.dolphin = dolphin
        self.api = SimpleNamespace(public=public or StubPublic(), trade=trade or StubTrade())

    def getLastAccount(self):
        frame = self.dolphin.runScript("shareAccountTable")
        return frame.set_index("ccy").T.to_dict()


def product_frame(lotSz="0.1", minSz="0.1", tickSz="0.01", ctVal="0.01"):
    return pd.DataFrame([{"lotSz": float(lotSz), "minSz": float(minSz),
                          "tickSz": float(tickSz), "ctVal": float(ctVal)}])


def position_frame(**overrides):
    row = {
        "instId": "BTC-USDT-SWAP", "posSide": "net", "pos": "1",
        "mgnMode": "isolated", "ccy": "USDT", "lever": "10",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_kernel_exposes_trader(monkeypatch):
    class FakeDolphin:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    kernel = Kernel(flag="1", private=False, instIds=set())
    assert isinstance(kernel.trader, Trader)


def test_buy_rounds_size_to_lot_and_passes_market_order():
    trade = StubTrade()
    trader = Trader(StubKernel(StubDolphin(product=product_frame()), trade=trade))
    packet = trader.buy("BTC-USDT-SWAP", 1.234)
    assert packet["code"] == "0"
    call = trade.place_calls[0]
    assert call["instId"] == "BTC-USDT-SWAP"
    assert call["side"] == "buy"
    assert call["ordType"] == "market"
    assert call["sz"] == "1.2"
    assert call["px"] == ""


def test_buy_rejects_size_below_min():
    trade = StubTrade()
    trader = Trader(StubKernel(StubDolphin(product=product_frame(lotSz="0.1", minSz="1")), trade=trade))
    with pytest.raises(ValueError):
        trader.buy("BTC-USDT-SWAP", 0.5)
    assert trade.place_calls == []


def test_limit_buy_requires_and_rounds_price():
    trade = StubTrade()
    trader = Trader(StubKernel(StubDolphin(product=product_frame()), trade=trade))
    with pytest.raises(ValueError):
        trader.buy("BTC-USDT-SWAP", 1, ordType="limit")
    packet = trader.buy("BTC-USDT-SWAP", 1, px=100.005, ordType="limit")
    assert packet["code"] == "0"
    call = trade.place_calls[0]
    assert call["px"] == "100.01"
    assert call["ordType"] == "limit"


def test_sell_uses_rest_fallback_when_db_missing():
    public = StubPublic(data=[{"instId": "BTC-USDT-SWAP", "lotSz": "0.01",
                               "minSz": "0.01", "tickSz": "0.1"}])
    trade = StubTrade()
    trader = Trader(StubKernel(StubDolphin(), public=public, trade=trade))
    trader.sell("BTC-USDT-SWAP", 0.019)
    assert public.calls == [("SWAP", "BTC-USDT-SWAP")]
    assert trade.place_calls[0]["sz"] == "0.01"
    assert trade.place_calls[0]["side"] == "sell"


def test_cl_ord_id_generated_and_bounded():
    trade = StubTrade()
    trader = Trader(StubKernel(StubDolphin(product=product_frame()), trade=trade))
    trader.buy("BTC-USDT-SWAP", 1)
    trader.buy("BTC-USDT-SWAP", 1)
    ids = [call["clOrdId"] for call in trade.place_calls]
    assert all(i and len(i) <= 32 and i.isalnum() for i in ids)
    assert ids[0] != ids[1]


def test_close_uses_position_snapshot_fields():
    trade = StubTrade()
    dolphin = StubDolphin(positions=position_frame(mgnMode="isolated", posSide="long"))
    trader = Trader(StubKernel(dolphin, trade=trade))
    packet = trader.close("BTC-USDT-SWAP")
    assert packet["code"] == "0"
    assert trade.close_calls == [
        {"instId": "BTC-USDT-SWAP", "mgnMode": "isolated", "posSide": "long", "ccy": "USDT"}
    ]


def test_get_position_and_account():
    account = pd.DataFrame([{"ccy": "USDT", "availBal": 1000.0}])
    trader = Trader(StubKernel(StubDolphin(positions=position_frame(pos="2"), account=account)))
    position = trader.getPosition("BTC-USDT-SWAP")
    assert position["pos"] == "2"
    assert trader.getPosition("ETH-USDT-SWAP") is None
    assert trader.getAccount("USDT")["availBal"] == 1000.0
    assert "USDT" in trader.getAccount()

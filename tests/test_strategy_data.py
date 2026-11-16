"""策略数据访问 API 测试（离线：FakeDolphin）。"""
import pandas as pd

from tradeflow.kernel import Kernel
from tradeflow.simulation import FakeApi, FakeDolphin
from tradeflow.strategy import Strategy


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


def make_strategy(tables):
    kernel = Kernel(flag="1", private=False, warmup=0, instIds=set(),
                    dolphin=FakeDolphin(tables), api=FakeApi())
    return DummyStrategy(kernel)


def bars_frame():
    rows = []
    start = pd.Timestamp("2026-01-01 00:00:00")
    for i in range(4):
        for inst in ("BTC-USDT-SWAP", "ETH-USDT-SWAP"):
            rows.append({"ts": start + pd.Timedelta(minutes=i), "o": 1.0, "h": 2.0,
                         "l": 0.5, "c": float(i), "vol": 1.0,
                         "volCcy": 1.0, "volCcyQuote": 1.0,
                         "instId": inst, "channel": "candle1m"})
    rows.append({"ts": start, "o": 1.0, "h": 2.0, "l": 0.5, "c": 9.0,
                 "vol": 1.0, "volCcy": 1.0, "volCcyQuote": 1.0,
                 "instId": "BTC-USDT-SWAP", "channel": "candle5m"})
    return pd.DataFrame(rows)


def test_get_bars_latest_per_instrument():
    strategy = make_strategy({"candleConfirmStreamTable": bars_frame()})
    frame = strategy.getBars("candle1m", limit=2)
    assert set(frame["channel"]) == {"candle1m"}
    assert len(frame) == 4  # 2 个品种 x 各最近 2 根
    for inst in ("BTC-USDT-SWAP", "ETH-USDT-SWAP"):
        closes = list(frame[frame["instId"] == inst]["c"])
        assert closes == [2.0, 3.0]
    assert list(frame["ts"]) == sorted(frame["ts"])  # 升序


def test_get_bars_single_instrument_filter():
    strategy = make_strategy({"candleConfirmStreamTable": bars_frame()})
    frame = strategy.getBars("candle1m", limit=10, instId="BTC-USDT-SWAP")
    assert set(frame["instId"]) == {"BTC-USDT-SWAP"}
    assert list(frame["c"]) == [0.0, 1.0, 2.0, 3.0]


def test_get_tick():
    ticks = pd.DataFrame([{"instId": "BTC-USDT-SWAP", "last": 100.0},
                          {"instId": "ETH-USDT-SWAP", "last": 10.0}])
    strategy = make_strategy({"shareTickTable": ticks})
    assert strategy.getTick("BTC-USDT-SWAP")["last"] == 100.0
    assert strategy.getTick("DOGE-USDT-SWAP") == {}


def test_get_indicators_default_table():
    start = pd.Timestamp("2026-01-01 00:00:00")
    rows = [
        {"ts": start + pd.Timedelta(minutes=i), "instId": "BTC-USDT-SWAP",
         "channel": "candle1m", "ma5": float(i), "ma10": 1.0}
        for i in range(3)
    ]
    strategy = make_strategy({"candleIndicatorStreamTable": pd.DataFrame(rows)})
    frame = strategy.getIndicators("candle1m", limit=2)
    assert list(frame["ma5"]) == [1.0, 2.0]


def test_get_book_latest_snapshot():
    ts_old = pd.Timestamp("2026-01-01 00:00:00")
    ts_new = pd.Timestamp("2026-01-01 00:01:00")
    rows = [
        {"instId": "BTC-USDT-SWAP", "source": "books5", "ts": ts_old,
         "side": "bid", "level": 1, "px": 99.0, "sz": 5.0},
        {"instId": "BTC-USDT-SWAP", "source": "books5", "ts": ts_new,
         "side": "bid", "level": 1, "px": 100.0, "sz": 2.0},
        {"instId": "BTC-USDT-SWAP", "source": "books5", "ts": ts_new,
         "side": "bid", "level": 2, "px": 99.0, "sz": 3.0},
        {"instId": "BTC-USDT-SWAP", "source": "books5", "ts": ts_new,
         "side": "ask", "level": 1, "px": 101.0, "sz": 1.0},
    ]
    strategy = make_strategy({"bookStreamTable": pd.DataFrame(rows)})
    book = strategy.getBook("BTC-USDT-SWAP")
    assert book["ts"] == ts_new
    assert book["bids"] == [(100.0, 2.0), (99.0, 3.0)]
    assert book["asks"] == [(101.0, 1.0)]
    assert strategy.getBook("ETH-USDT-SWAP")["bids"] == []


def test_get_orders_filter():
    orders = pd.DataFrame([
        {"instId": "BTC-USDT-SWAP", "ordId": "1", "state": "live"},
        {"instId": "BTC-USDT-SWAP", "ordId": "2", "state": "filled"},
        {"instId": "ETH-USDT-SWAP", "ordId": "3", "state": "live"},
    ])
    strategy = make_strategy({"shareOrderTable": orders})
    assert [o["ordId"] for o in strategy.getOrders("BTC-USDT-SWAP", state="live")] == ["1"]
    assert len(strategy.getOrders()) == 3

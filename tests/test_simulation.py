"""离线模拟工具测试：FakeDolphin / FakeApi 支持不连库不联网跑策略。"""
import pandas as pd
import pytest

from tradeflow.kernel import Kernel
from tradeflow.simulation import FakeApi, FakeDolphin
from tradeflow.strategy import Strategy


@pytest.fixture
def tables():
    return {
        "shareTickTable": pd.DataFrame([
            {"instId": "BTC-USDT-SWAP", "last": 100.0, "ts": pd.Timestamp("2026-01-01 00:00:00")},
            {"instId": "ETH-USDT-SWAP", "last": 10.0, "ts": pd.Timestamp("2026-01-01 00:00:00")},
        ]),
        "candleIndicatorStreamTable": pd.DataFrame([
            {"instId": "BTC-USDT-SWAP", "channel": "candle1m",
             "ts": pd.Timestamp("2026-01-01 00:00:00"), "ma5": 1.0},
            {"instId": "BTC-USDT-SWAP", "channel": "candle1m",
             "ts": pd.Timestamp("2026-01-01 00:01:00"), "ma5": 2.0},
            {"instId": "BTC-USDT-SWAP", "channel": "candle1m",
             "ts": pd.Timestamp("2026-01-01 00:02:00"), "ma5": 3.0},
            {"instId": "BTC-USDT-SWAP", "channel": "candle5m",
             "ts": pd.Timestamp("2026-01-01 00:00:00"), "ma5": 9.0},
        ]),
        "shareTickIndicatorTable": pd.DataFrame([
            {"instId": "BTC-USDT-SWAP", "count5m": 10},
            {"instId": "ETH-USDT-SWAP", "count5m": 30},
        ]),
    }


def test_fake_dolphin_query_patterns(tables):
    dolphin = FakeDolphin(tables)
    frame = dolphin.runScript("shareTickTable")
    assert set(frame["instId"]) == {"BTC-USDT-SWAP", "ETH-USDT-SWAP"}
    count = dolphin.runScript("exec count(*) from shareTickTable")
    assert count == 2
    instids = dolphin.runScript(
        "exec instId from shareTickIndicatorTable order by count5m desc limit 1"
    )
    assert list(instids) == ["ETH-USDT-SWAP"]
    filtered = dolphin.runScript(
        "select * from candleIndicatorStreamTable where channel='candle5m'"
    )
    assert len(filtered) == 1


def test_fake_dolphin_rejects_unknown_script(tables):
    dolphin = FakeDolphin(tables)
    with pytest.raises(NotImplementedError):
        dolphin.runScript("select ts, price from someUnknownTable context by x")


def test_fake_dolphin_top_order_by_with_two_filters(tables):
    tables = dict(tables)
    tables["candleConfirmStreamTable"] = pd.DataFrame([
        {"instId": "BTC-USDT-SWAP", "channel": "candle1m",
         "ts": pd.Timestamp("2026-01-01 00:00:00") + pd.Timedelta(minutes=i),
         "c": float(i)}
        for i in range(5)
    ] + [
        {"instId": "ETH-USDT-SWAP", "channel": "candle1m",
         "ts": pd.Timestamp("2026-01-01 00:09:00"), "c": 99.0},
    ])
    dolphin = FakeDolphin(tables)
    frame = dolphin.runScript(
        "select top 2 ts, c from candleConfirmStreamTable "
        "where channel='candle1m' and instId='BTC-USDT-SWAP' order by ts desc"
    )
    assert list(frame.columns) == ["ts", "c"]
    assert list(frame["c"]) == [4.0, 3.0]


def test_fake_dolphin_context_top_per_group():
    start = pd.Timestamp("2026-01-01 00:00:00")
    rows = []
    for inst in ("BTC-USDT-SWAP", "ETH-USDT-SWAP"):
        for i in range(3):
            rows.append({"instId": inst, "channel": "candle1m",
                         "ts": start + pd.Timedelta(minutes=i), "c": float(i)})
    dolphin = FakeDolphin({"candleConfirmStreamTable": pd.DataFrame(rows)})
    frame = dolphin.runScript(
        "select top 2 ts, c, instId from candleConfirmStreamTable "
        "where channel='candle1m' context by instId csort ts desc"
    )
    assert list(frame.columns) == ["ts", "c", "instId"]
    assert len(frame) == 4  # 每个品种最近 2 根
    for inst in ("BTC-USDT-SWAP", "ETH-USDT-SWAP"):
        closes = list(frame[frame["instId"] == inst]["c"])
        assert closes == [1.0, 2.0]  # 结果按 ts 升序
    assert list(frame["ts"]) == sorted(frame["ts"])


def test_fake_dolphin_exec_max(tables):
    dolphin = FakeDolphin(tables)
    value = dolphin.runScript("exec max(ts) from shareTickTable")
    assert value == pd.Timestamp("2026-01-01 00:00:00")


class OfflineStrategy(Strategy):
    instIds = {"BTC-USDT-SWAP"}

    def __init__(self, kernel):
        super().__init__(kernel)
        self.scanned = False
        self.snapshot = {}

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
        self.scanned = True
        self.snapshot["ticks"] = self.getLastTicks()
        indicator = self.getCandleIndicator(limit=2, channel="candle1m")
        self.snapshot["ma5"] = list(indicator["BTC-USDT-SWAP"]["ma5"])
        self.snapshot["top"] = self.kernel.getTradeInstid(limit=1)


def test_strategy_runs_offline_with_fake_environment(tables):
    kernel = Kernel(
        flag="1", private=False, warmup=0, instIds=set(),
        dolphin=FakeDolphin(tables), api=FakeApi(),
    )
    kernel.addStrategy(OfflineStrategy)
    kernel.run()
    strategy = kernel.strategies[0]
    assert strategy.scanned is True
    assert strategy.snapshot["ticks"]["BTC-USDT-SWAP"]["last"] == 100.0
    # context by ... limit 2 取每个品种最近 2 根，再按 ts 升序
    assert strategy.snapshot["ma5"] == [2.0, 3.0]
    assert strategy.snapshot["top"] == ["ETH-USDT-SWAP"]


def test_strategy_trades_offline_with_fake_api(tables):
    api = FakeApi(instruments={
        "BTC-USDT-SWAP": {"instId": "BTC-USDT-SWAP", "lotSz": "0.1",
                          "minSz": "0.1", "tickSz": "0.01"},
    })
    kernel = Kernel(
        flag="1", private=False, warmup=0, instIds=set(),
        dolphin=FakeDolphin(tables), api=api,
    )
    kernel.addStrategy(OfflineStrategy)
    kernel.run()
    strategy = kernel.strategies[0]
    packet = strategy.buy("BTC-USDT-SWAP", 1.23)
    assert packet["code"] == "0"
    assert api.trade.orders[0]["sz"] == "1.2"


def test_trader_uses_product_table_before_rest(tables):
    tables = dict(tables)
    tables["productStreamTable"] = pd.DataFrame([
        {"instId": "BTC-USDT-SWAP", "lotSz": 0.1, "minSz": 0.1,
         "tickSz": 0.01, "ctVal": 0.01},
    ])
    api = FakeApi()  # 无 instruments：若走 REST 回退会因无规格而失败
    kernel = Kernel(
        flag="1", private=False, warmup=0, instIds=set(),
        dolphin=FakeDolphin(tables), api=api,
    )
    packet = kernel.trader.buy("BTC-USDT-SWAP", 2.34)
    assert packet["code"] == "0"
    assert api.trade.orders[0]["sz"] == "2.3"

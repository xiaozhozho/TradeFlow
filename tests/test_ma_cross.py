"""MA5/MA15 均线交叉模拟盘策略测试（离线：FakeDolphin + FakeApi）。

策略对多个合约独立生效；指标由框架声明式计算
（indicators = {"ma": [5, 15]}），离线环境直接预置
indicator_MaCrossStrategyStreamTable 模拟流引擎输出。
"""
import logging

import pandas as pd

from strategy.ma_cross import (
    INST_IDS,
    MaCrossStrategy,
    cross_signal,
)
from tradeflow.kernel import Kernel
from tradeflow.simulation import FakeApi, FakeDolphin

INDICATOR_TABLE = "indicator_MaCrossStrategyStreamTable"
INST_ID = "BTC-USDT-SWAP"
ETH_ID = "ETH-USDT-SWAP"

# 各测试合约的 (最新价, ctVal)，使 price x ctVal = 1 -> 10U x 30 倍 = 300 张
SPECS = {
    INST_ID: {"price": 100.0, "lotSz": "0.01", "minSz": "0.01", "tickSz": "0.1", "ctVal": "0.01"},
    ETH_ID: {"price": 10.0, "lotSz": "0.01", "minSz": "0.01", "tickSz": "0.01", "ctVal": "0.1"},
}


def test_strategy_declares_candles_and_indicators():
    assert MaCrossStrategy.instIds == set(INST_IDS)
    assert {"BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP",
            "DOGE-USDT-SWAP", "XRP-USDT-SWAP"} <= set(INST_IDS)
    assert MaCrossStrategy.candles == ("candle1m",)
    assert MaCrossStrategy.indicators == {"ma": [5, 15]}


def test_cross_signal_golden():
    assert cross_signal(100.0, 100.0, 102.0, 100.67) == "long"


def test_cross_signal_death():
    assert cross_signal(100.0, 100.0, 98.0, 99.33) == "short"


def test_cross_signal_none():
    # 持续多头排列，无交叉
    assert cross_signal(102.0, 100.0, 103.0, 101.0) is None
    # NaN（指标未成形）不产生信号
    assert cross_signal(float("nan"), 100.0, 102.0, 100.67) is None


GOLDEN = [(100.0, 100.0), (102.0, 100.67)]
# 死叉发生在最后一根（ts 晚于 GOLDEN，保证与新信号去重逻辑兼容）
DEATH = [(102.0, 100.67), (100.0, 100.0), (98.0, 99.33)]


def _indicator_rows(pairs):
    return _multi_indicator_rows({INST_ID: pairs})


def _multi_indicator_rows(pairs_by_inst):
    start = pd.Timestamp("2026-01-01 00:00:00")
    rows = []
    for inst_id, pairs in pairs_by_inst.items():
        for index, (fast, slow) in enumerate(pairs):
            rows.append({"ts": start + pd.Timedelta(minutes=index), "instId": inst_id,
                         "channel": "candle1m", "ma_5": fast, "ma_15": slow})
    return pd.DataFrame(rows)


def _long_position(inst_id=INST_ID):
    return pd.DataFrame([
        {"instId": inst_id, "posSide": "net", "pos": "5",
         "mgnMode": "cross", "ccy": "USDT", "lever": "30"},
    ])


def make_kernel(pairs=None, positions=None, pairs_by_inst=None, tick_insts=None):
    """pairs: 单合约（BTC）指标序列；pairs_by_inst: 多合约 {instId: pairs}。"""
    pairs_by_inst = pairs_by_inst or {INST_ID: pairs or GOLDEN}
    if tick_insts is None:
        tick_insts = [inst for inst in pairs_by_inst if inst in SPECS]
    tables = {
        INDICATOR_TABLE: _multi_indicator_rows(pairs_by_inst),
        "tickStreamTable": pd.DataFrame(),
        "shareTickTable": pd.DataFrame([
            {"instId": inst, "last": SPECS[inst]["price"]} for inst in tick_insts
        ]),
        "sharePositionTable": positions if positions is not None else pd.DataFrame(
            columns=["instId", "posSide", "pos", "mgnMode", "ccy", "lever"]
        ),
    }
    instruments = {
        inst: {"instId": inst, **{k: v for k, v in SPECS[inst].items() if k != "price"}}
        for inst in pairs_by_inst if inst in SPECS
    }
    kernel = Kernel(flag="1", private=False, warmup=0, instIds=set(),
                    dolphin=FakeDolphin(tables), api=FakeApi(instruments=instruments))
    kernel.risk.lever = 30
    kernel.addStrategy(MaCrossStrategy)
    return kernel


def test_golden_cross_opens_long_once():
    kernel = make_kernel(GOLDEN)
    kernel.run()
    kernel.run()  # 同一根 K 线的交叉只执行一次
    orders = kernel.api.trade.orders
    assert len(orders) == 1
    assert orders[0]["side"] == "buy"
    # 10U 保证金 x 30 倍 / (100 x ctVal 0.01) = 300 张
    assert float(orders[0]["sz"]) == 300.0


def test_death_cross_flips_long_to_short():
    kernel = make_kernel(DEATH, _long_position())
    kernel.run()
    assert len(kernel.api.trade.closes) == 1
    orders = kernel.api.trade.orders
    assert len(orders) == 1
    assert orders[0]["side"] == "sell"
    assert float(orders[0]["sz"]) == 300.0


def test_same_side_signal_no_action():
    kernel = make_kernel(GOLDEN, _long_position())
    kernel.run()
    kernel.run()
    assert kernel.api.trade.orders == []
    assert kernel.api.trade.closes == []


def test_insufficient_indicators_waits():
    kernel = make_kernel([(100.0, 100.0)])
    kernel.run()
    assert kernel.api.trade.orders == []


def test_scan_logs_tick_table_count(caplog):
    kernel = make_kernel([(100.0, 100.0)])
    kernel.dolphin.update(
        "tickStreamTable",
        pd.DataFrame([{"instId": INST_ID, "last": 100.0}] * 7),
    )
    with caplog.at_level(logging.INFO):
        kernel.run()
    assert any("tickStreamTable 数据量: 7" in record.message for record in caplog.records)


def test_sequential_cross_round_trip():
    """先金叉开多；模拟成交后，死叉平多反手做空。"""
    kernel = make_kernel(GOLDEN)
    kernel.run()
    assert kernel.api.trade.orders[0]["side"] == "buy"

    kernel.dolphin.update("sharePositionTable", _long_position())
    kernel.dolphin.update(INDICATOR_TABLE, _indicator_rows(DEATH))
    kernel.run()

    assert len(kernel.api.trade.closes) == 1                    # 先平多
    assert kernel.api.trade.orders[-1]["side"] == "sell"        # 再反手做空
    assert float(kernel.api.trade.orders[-1]["sz"]) == 300.0


def test_multi_instrument_signals_are_independent():
    kernel = make_kernel(pairs_by_inst={INST_ID: GOLDEN, ETH_ID: DEATH})
    kernel.run()
    orders = kernel.api.trade.orders
    assert len(orders) == 2
    by_inst = {order["instId"]: order for order in orders}
    assert by_inst[INST_ID]["side"] == "buy"
    assert by_inst[ETH_ID]["side"] == "sell"
    # 相同 10U 保证金，按各自价格与 ctVal 换算后均为 300 张
    assert float(by_inst[INST_ID]["sz"]) == 300.0
    assert float(by_inst[ETH_ID]["sz"]) == 300.0
    # 同一根 K 线的交叉按品种各自去重
    kernel.run()
    assert len(kernel.api.trade.orders) == 2


def test_same_side_skip_is_per_instrument():
    kernel = make_kernel(pairs_by_inst={INST_ID: GOLDEN, ETH_ID: GOLDEN},
                         positions=_long_position(INST_ID))
    kernel.run()
    orders = kernel.api.trade.orders
    assert len(orders) == 1
    assert orders[0]["instId"] == ETH_ID  # BTC 已持多仓跳过，ETH 正常开多


def test_one_instrument_failure_does_not_block_others():
    # ETH 没有 tick 快照 -> 无法定价；BTC 仍应正常下单
    kernel = make_kernel(pairs_by_inst={INST_ID: GOLDEN, ETH_ID: DEATH},
                         tick_insts=[INST_ID])
    kernel.run()
    orders = kernel.api.trade.orders
    assert len(orders) == 1
    assert orders[0]["instId"] == INST_ID

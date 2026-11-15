"""DolphinDB 流表/引擎集成测试。

⚠️ 本测试会 drop/recreate 全部流表，必须满足：
- TRADEFLOW_TEST_DDB=1
- TRADEFLOW_TEST_DDB_ALLOW_DROP=1
且 DOLPHINDB_HOST 指向**独立的测试实例**，不要对
strategy/example.py 正在使用的库执行。
"""
import os
import time

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("TRADEFLOW_TEST_DDB") != "1"
    or os.getenv("TRADEFLOW_TEST_DDB_ALLOW_DROP") != "1",
    reason="需要独立的 DolphinDB 测试实例，并显式允许删表",
)


def test_stream_tables_engines_and_dataflow():
    from tradeflow.stream.dolphin import Dolphin
    from tradeflow.stream.engines import ENGINE_SPECS
    from tradeflow.stream.schema import STREAM_TABLES

    d = Dolphin()
    # 先清理引擎与订阅（引擎持有输出表引用，会阻塞 dropStreamTable）
    for spec in ENGINE_SPECS.values():
        d.unsubscribeTable(spec.source, spec.action)
        d.dropStreamEngine(spec.name)
    d.prepare_streams(STREAM_TABLES.keys(), recreate=True)
    d.prepare_engines((
        "lastTickAction", "lastCandleAction",
        "candleIndicatorEngine", "tickIndicatorEngine",
        "lastTickIndicatorAction", "lastCandleIndicatorAction",
    ))
    time.sleep(1)

    # 1. 表与快照表存在
    assert d.runScript("existsStreamTable(`tickStreamTable)") is True
    assert d.runScript("existsStreamTable(`candleConfirmStreamTable)") is True
    assert d.runScript("defined(`shareCandleIndicatorTable, SHARED)") is True
    assert d.runScript("defined(`shareTickIndicatorTable, SHARED)") is True

    # 2. 订阅已注册（getStreamingStat().pubTables 含 actions 列）
    pubs = d.runScript("getStreamingStat().pubTables")
    actions = set(pubs["actions"])
    assert {"lastTickAction", "lastCandleAction", "candleIndicatorAction",
            "tickIndicatorAction"} <= actions

    # 3. 响应式状态引擎：K 线指标（mavg 带 minPeriods=1，未满窗也输出）
    d.runScript("""
    insert into candleConfirmStreamTable values(2024.01.01T00:01:00.000, 1, 2, 0.5, 1.5, 10, 10, 15, 1, "BTC-USDT-SWAP", "candle1m")
    insert into candleConfirmStreamTable values(2024.01.01T00:02:00.000, 1.5, 2.5, 1, 2, 11, 11, 16, 1, "BTC-USDT-SWAP", "candle1m")
    insert into candleConfirmStreamTable values(2024.01.01T00:03:00.000, 2, 3, 1.5, 2.5, 12, 12, 17, 1, "BTC-USDT-SWAP", "candle1m")
    """)
    time.sleep(2)
    ind = d.runScript(
        "select * from candleIndicatorStreamTable order by ts"
    )
    assert len(ind) == 3
    # ma5 用 minPeriods=1：第一根就有值（等于 c 本身）
    assert ind.loc[0, "ma5"] == pytest.approx(1.5)
    assert ind.loc[2, "ma5"] == pytest.approx((1.5 + 2 + 2.5) / 3)
    # 输出列为 分组列 -> 时间列 -> 指标列
    assert list(ind.columns[:4]) == ["instId", "channel", "ts", "ma5"]

    # 4. 最新快照动作：lastCandleAction 更新 shareCandleTable
    d.runScript("""
    insert into candleStreamTable values(2024.01.01T00:04:00.000, 2.5, 3.5, 2, 3, 13, 13, 18, 0, "BTC-USDT-SWAP", "candle1m")
    """)
    time.sleep(2)
    latest = d.runScript(
        "select * from shareCandleTable where instId='BTC-USDT-SWAP'"
    )
    assert len(latest) == 1
    assert latest.loc[0, "c"] == 3.0

    # 5. 时间序列引擎：tick 5 分钟活跃度（窗口结束时间输出）
    d.runScript("""
    insert into tickStreamTable values(`SWAP, "BTC-USDT-SWAP", 100.0, 1.0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 2024.01.01T00:00:10.000)
    insert into tickStreamTable values(`SWAP, "BTC-USDT-SWAP", 101.0, 2.0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 2024.01.01T00:01:10.000)
    insert into tickStreamTable values(`SWAP, "BTC-USDT-SWAP", 102.0, 3.0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 2024.01.01T00:04:10.000)
    insert into tickStreamTable values(`SWAP, "BTC-USDT-SWAP", 103.0, 4.0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 2024.01.01T00:06:10.000)
    """)
    time.sleep(2)
    tick_ind = d.runScript("select * from tickIndicatorStreamTable order by ts")
    assert len(tick_ind) >= 1
    row_0001 = tick_ind[tick_ind["ts"] == d.runScript("2024.01.01T00:01:00.000")]
    assert len(row_0001) == 1
    assert row_0001.iloc[0]["count5m"] == 1
    assert row_0001.iloc[0]["close5m"] == pytest.approx(100.0)
    row_0005 = tick_ind[tick_ind["ts"] == d.runScript("2024.01.01T00:05:00.000")]
    assert len(row_0005) == 1
    assert row_0005.iloc[0]["count5m"] == 3
    assert row_0005.iloc[0]["close5m"] == pytest.approx(102.0)
    row_0006 = tick_ind[tick_ind["ts"] == d.runScript("2024.01.01T00:06:00.000")]
    assert len(row_0006) == 1
    assert row_0006.iloc[0]["count5m"] == 2
    assert row_0006.iloc[0]["close5m"] == pytest.approx(102.0)

    # 6. lastTickAction：shareTickTable 每 instId 只保留最新一行
    share = d.runScript("select * from shareTickTable")
    assert len(share) == 1
    assert share.loc[0, "last"] == pytest.approx(103.0)

    # 7. shareTickIndicatorTable 快照 = tickIndicatorStreamTable 最新窗口（00:06）
    share_ind = d.runScript("select * from shareTickIndicatorTable")
    assert len(share_ind) == 1
    assert share_ind.loc[0, "ts"] == d.runScript("2024.01.01T00:06:00.000")
    assert share_ind.loc[0, "count5m"] == 2
    assert share_ind.loc[0, "close5m"] == pytest.approx(102.0)

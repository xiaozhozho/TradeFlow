"""stream.schema 表结构注册表测试。"""
import pandas as pd

from tradeflow.stream.schema import STREAM_TABLES, FUNDING, TICK


EXPECTED_TABLES = {
    # 公共行情
    "tickStreamTable", "shareTickTable",
    "candleStreamTable", "candleConfirmStreamTable", "shareCandleTable",
    "openinterestStreamTable", "fundingrateStreamTable",
    "markpriceStreamTable", "markpricecandleStreamTable",
    "indextickStreamTable", "indexcandleStreamTable",
    "pricelimitStreamTable", "optsummaryStreamTable",
    "estimatedpriceStreamTable", "optiontradeStreamTable",
    "liquidationorderStreamTable", "statusStreamTable", "productStreamTable",
    # 盘口/逐笔
    "tradeStreamTable", "bookStreamTable",
    # 指标
    "candleIndicatorStreamTable", "shareCandleIndicatorTable",
    "tickIndicatorStreamTable", "shareTickIndicatorTable",
    # 私有
    "orderStreamTable", "shareOrderTable",
    "algoOrderStreamTable", "shareAlgoOrderTable",
    "fillsStreamTable",
    "shareAccountTable", "sharePositionTable", "shareBalanceTable",
    "shareWarningTable",
    "accountGreeksStreamTable", "shareAccountGreeksTable",
}


def test_all_expected_tables_registered():
    missing = EXPECTED_TABLES - set(STREAM_TABLES)
    assert not missing, f"缺少表定义: {missing}"


def test_tick_columns_order():
    """旧表列顺序保持不变。"""
    assert TICK.names[:3] == ["instType", "instId", "last"]
    assert TICK.types[-1] == "TIMESTAMP"
    assert len(TICK.names) == 16


def test_order_stream_columns_preserved():
    """orderStreamTable 63 列原样迁移。"""
    order = STREAM_TABLES["orderStreamTable"]
    assert len(order.names) == 63
    assert order.names[0] == "accFillSz"
    assert order.names[-1] == "uTime"
    assert order.types[-1] == "TIMESTAMP"


def test_funding_empty_next_rate():
    df = pd.DataFrame([{
        "instId": "BTC-USD-SWAP",
        "instType": "SWAP",
        "fundingRate": "0.0001",
        "fundingTime": "1704067200000",
        "nextFundingRate": "",
        "nextFundingTime": "1704096000000",
        "ts": "1704067200000",
    }])
    out = FUNDING.normalize(df)
    assert out.loc[0, "nextFundingRate"] == 0.0
    assert out.loc[0, "fundingRate"] == 0.0001
    assert str(out["ts"].dtype).startswith("datetime64")


def test_new_snapshot_tables_are_latest():
    for name, keys, time_col in (
        ("shareCandleTable", ("instId", "channel"), "ts"),
        ("shareCandleIndicatorTable", ("instId", "channel"), "ts"),
        ("shareTickIndicatorTable", ("instId",), "ts"),
        ("shareOrderTable", ("ordId",), "uTime"),
        ("shareAlgoOrderTable", ("algoId",), "uTime"),
        ("shareAccountGreeksTable", ("ccy",), "ts"),
    ):
        spec = STREAM_TABLES[name]
        assert spec.latest is True, name
        assert spec.key_columns == keys, name
        assert spec.time_column == time_col, name


def test_create_script_is_idempotent():
    script = TICK.ensure_script(recreate=False)
    assert "existsStreamTable(`tickStreamTable)" in script
    assert "enableTableShareAndPersistence" in script
    assert "dropStreamTable" not in script


def test_ensure_script_recreate_contains_drop():
    script = TICK.ensure_script(recreate=True)
    assert "dropStreamTable(`tickStreamTable)" in script
    assert "existsStreamTable(`tickStreamTable)" in script


def test_latest_table_script_uses_defined_check():
    spec = STREAM_TABLES["shareOrderTable"]
    script = spec.ensure_script(recreate=False)
    assert "latestIndexedTable" in script
    assert "defined(`shareOrderTable, SHARED)" in script

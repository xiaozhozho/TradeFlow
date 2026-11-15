"""utils.format 纯函数单元测试：不依赖网络与 DolphinDB。"""
import pandas as pd

from tradeflow.utils.format import align_frame, as_float, as_int, ts2Timestamp


def test_ts2timestamp_utc():
    """OKX 毫秒时间戳 -> UTC-naive Timestamp。"""
    assert ts2Timestamp("1704067200000") == pd.Timestamp("2024-01-01 00:00:00")
    assert ts2Timestamp(1704067200000) == pd.Timestamp("2024-01-01 00:00:00")


def test_ts2timestamp_empty_and_zero():
    assert pd.isna(ts2Timestamp(""))
    assert pd.isna(ts2Timestamp(None))
    assert pd.isna(ts2Timestamp(0))


def test_empty_numeric():
    assert as_float("") == 0.0
    assert as_float("unsupported") == 0.0
    assert as_float(None) == 0.0
    assert as_int("12.9") == 12
    assert as_int("") == 0


def test_align_frame_orders_and_casts():
    df = pd.DataFrame({"b": ["2"], "a": [""]})
    out = align_frame(df, ["a", "b"], {"a": "float", "b": "int"})
    assert list(out.columns) == ["a", "b"]
    assert out.loc[0, "a"] == 0.0
    assert out.loc[0, "b"] == 2


def test_align_frame_missing_columns_filled():
    df = pd.DataFrame({"a": ["1.5"]})
    out = align_frame(df, ["a", "b", "ts"], {"a": "float", "b": "int", "ts": "ts"})
    assert list(out.columns) == ["a", "b", "ts"]
    assert out.loc[0, "a"] == 1.5
    assert out.loc[0, "b"] == 0
    assert pd.isna(out.loc[0, "ts"])


def test_align_frame_bool_strings():
    """OKX 推送 'true'/'false' 字符串，必须映射为布尔而非 astype(bool)。"""
    df = pd.DataFrame({"flag": ["true", "false", None, "TRUE"]})
    out = align_frame(df, ["flag"], {"flag": "bool"})
    assert out["flag"].tolist() == [True, False, False, True]

"""订阅数据格式化纯函数。

只做数据转换，不依赖 Kernel、DolphinDB、网络，方便单测与复用。
约定：

- 数值字段空串（OKX 用 ``""`` 表示无值）统一转默认值，不抛异常；
- 时间字段统一转 UTC-naive ``pd.Timestamp``（OKX 毫秒时间戳），入库由
  ``TableSpec.normalize`` 负责；
- ``align_frame`` 是所有 handler 落库前的统一出口，保证列顺序与类型正确。
"""
from __future__ import annotations

import json
from typing import Any, Iterable

import pandas as pd

_EMPTY = ("", "unsupported", "off", None)


def as_float(value: Any, default: float = 0.0) -> float:
    """空串/非法值转 default，其余转 float。"""
    if value in _EMPTY:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value: Any, default: int = 0) -> int:
    """空串/非法值转 default，其余转 int（兼容 '12.9' 这类字符串）。"""
    if value in _EMPTY:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any) -> bool:
    """OKX 布尔字段可能是 'true'/'false' 字符串，不能用 astype(bool)。"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("true", "1", "yes")


def ts2Timestamp(value: Any) -> pd.Timestamp:
    """OKX 毫秒时间戳 -> UTC-naive pandas Timestamp；空值返回 NaT。

    注意与旧版 ``utils.method.ts2Timestamp``（本地时区）语义不同：
    本函数是全项目唯一的时间转换入口，输出 UTC-naive TIMESTAMP。
    """
    if value in _EMPTY or value == 0:
        return pd.NaT
    if isinstance(value, float) and pd.isna(value):
        return pd.NaT
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is not None:
            return value.tz_convert("UTC").tz_localize(None)
        return value
    if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
        return pd.to_datetime(value, utc=True).tz_localize(None)
    return pd.to_datetime(int(float(value)), unit="ms", utc=True).tz_localize(None)


def align_frame(
    frame: pd.DataFrame,
    columns: Iterable[str],
    type_map: dict[str, str],
) -> pd.DataFrame:
    """按目标列顺序重排，并按 float/int/ts/bool/str 转换；缺失列补默认值。

    type_map 取值：'float' / 'int' / 'ts' / 'bool' / 其他按 str 处理。
    """
    out = frame.reindex(columns=list(columns))
    for column, kind in type_map.items():
        if column not in out.columns:
            continue
        if kind == "float":
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0).astype("float64")
        elif kind == "int":
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0).astype("int64")
        elif kind == "ts":
            out[column] = out[column].map(ts2Timestamp)
        elif kind == "bool":
            out[column] = out[column].map(_as_bool).astype(bool)
        else:
            out[column] = out[column].fillna("").astype(str)
    return out


def json_dumps(value: Any) -> str:
    """紧凑 JSON 序列化，用于保存原始报文（如 algo 单 raw 字段）。"""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

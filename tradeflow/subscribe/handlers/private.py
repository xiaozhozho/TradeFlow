"""私有频道解析：account / positions / orders / fills / 告警 / Greeks / 策略单。

约定：
- account / balance 的明细在 data[*].details / data[*].balData 里，需展开；
- orders / account-greeks / orders-algo 同时写流表（历史）与快照表（最新）；
- 列对齐与类型转换交给 TableSpec.normalize，这里只做结构展开与拼接。
"""
import pandas as pd

from ...utils.format import json_dumps
from .batch import Batch


def _data(packet):
    return packet.get("data") or []


def parse_account(packet):
    rows = []
    for item in _data(packet):
        rows.extend(item.get("details") or [])
    return [Batch("shareAccountTable", pd.DataFrame(rows))]


def parse_positions(packet):
    return [Batch("sharePositionTable", pd.DataFrame(_data(packet)))]


def parse_orders(packet):
    df = pd.DataFrame(_data(packet))
    return [
        Batch("orderStreamTable", df),
        Batch("shareOrderTable", df.copy()),
    ]


def parse_fills(packet):
    return [Batch("fillsStreamTable", pd.DataFrame(_data(packet)))]


def parse_balance(packet):
    rows = []
    for item in _data(packet):
        rows.extend(item.get("balData") or [])
    return [Batch("shareBalanceTable", pd.DataFrame(rows))]


def parse_liquidation_warning(packet):
    return [Batch("shareWarningTable", pd.DataFrame(_data(packet)))]


def parse_account_greeks(packet):
    df = pd.DataFrame(_data(packet))
    return [
        Batch("accountGreeksStreamTable", df),
        Batch("shareAccountGreeksTable", df.copy()),
    ]


def _join_ids(value):
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return value


def parse_algo_order(packet):
    """orders-algo / algo-advance 共用；列表字段拼接，原始报文存 raw。"""
    data = _data(packet)
    df = pd.DataFrame(data)
    for key in ("ordIdList", "subAlgoIdList"):
        if key in df.columns:
            df[key] = df[key].map(_join_ids)
    df["raw"] = [json_dumps(item) for item in data]
    return [
        Batch("algoOrderStreamTable", df),
        Batch("shareAlgoOrderTable", df.copy()),
    ]

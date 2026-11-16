"""Kernel.prepare 建表集合测试。

引擎的 source/target 表必须先于引擎创建，否则新库首次启动会因
subscribeTable / createReactiveStateEngine 引用不存在的表而失败。
"""
import pytest

from tradeflow.kernel import Kernel, prepare_table_set


class FakeDolphin:
    def __init__(self, *args, **kwargs):
        self.prepared_tables = []
        self.prepared_engines = []
        self.recreate_flags = []

    def prepare_streams(self, table_names, recreate=False):
        self.prepared_tables.extend(table_names)
        self.recreate_flags.append(recreate)

    def prepare_engines(self, engine_names):
        self.prepared_engines.extend(engine_names)

    def unsubscribeTable(self, tableName, actionName):
        pass

    def dropStreamEngine(self, engineName):
        pass


def make_kernel(monkeypatch, **kwargs):
    monkeypatch.setattr("tradeflow.kernel.Dolphin", FakeDolphin)
    return Kernel(flag="1", private=False, instIds={"BTC-USDT-SWAP"}, **kwargs)


def test_engine_tables_in_prepare_set(monkeypatch):
    kernel = make_kernel(monkeypatch)  # private=False, algo=False
    tables = prepare_table_set(kernel.enabled_channels())
    # 指标引擎的输入/输出表不隶属任何频道，也必须建
    assert {"candleIndicatorStreamTable", "tickIndicatorStreamTable",
            "shareCandleIndicatorTable", "shareTickIndicatorTable"} <= tables
    # 策略委托引擎无条件创建，其表也必须存在（即使 algo=False）
    assert {"algoOrderStreamTable", "shareAlgoOrderTable"} <= tables


def test_prepare_creates_engine_tables(monkeypatch):
    kernel = make_kernel(monkeypatch)
    kernel.prepare()
    tables = set(kernel.dolphin.prepared_tables)
    assert {"candleIndicatorStreamTable", "tickIndicatorStreamTable"} <= tables
    assert "algoOrderStreamTable" in tables
    # 流引擎请求已发出
    assert kernel.dolphin.prepared_engines


def test_prepare_recreate_flag_propagates(monkeypatch):
    kernel = make_kernel(monkeypatch, recreate_tables=True)
    kernel.prepare()
    assert kernel.dolphin.recreate_flags == [True]

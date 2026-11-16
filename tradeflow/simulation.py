"""离线开发与策略测试工具：不连 DolphinDB、不连 OKX 也能跑策略。

用法::

    from tradeflow import Kernel
    from tradeflow.simulation import FakeApi, FakeDolphin

    kernel = Kernel(flag="1", private=False, warmup=0,
                    dolphin=FakeDolphin(tables), api=FakeApi())
    kernel.addStrategy(MyStrategy)
    kernel.run()      # 策略 init() + scan() 全流程离线执行

``FakeDolphin`` 用 pandas 表模拟 ``Dolphin.runScript``，覆盖框架内置查询的
常见语句形态（裸表名、select、exec count、context by 窗口、top N 列）；
不认识的语句抛 ``NotImplementedError``（附原文），按需扩展即可。

``FakeApi`` 替代 ``Kernel.api``：``public.get_instruments`` 供 Trader 查
合约规格，``trade.place_order/close_positions`` 记录调用并返回成功应答，
断言用 ``api.trade.orders`` / ``api.trade.closes``。
"""
from __future__ import annotations

import re
from types import SimpleNamespace

import pandas as pd


class FakeDolphin:
    """用内存 pandas 表模拟 DolphinDB 查询。"""

    def __init__(self, tables=None):
        self.tables = {name: frame.copy() for name, frame in (tables or {}).items()}
        self.prepared_tables = None
        self.prepared_engines = None

    # ---------- 数据维护 ----------

    def update(self, name: str, frame: pd.DataFrame) -> None:
        """覆盖/写入一张内存表。"""
        self.tables[name] = frame.copy()

    def append(self, name: str, rows) -> None:
        """向内存表追加行（模拟流表持续写入）。"""
        incoming = pd.DataFrame(rows)
        if name in self.tables:
            self.tables[name] = pd.concat([self.tables[name], incoming], ignore_index=True)
        else:
            self.tables[name] = incoming

    # ---------- 与 Dolphin 相同的入口 ----------

    def prepare_streams(self, table_names, recreate=False):
        self.prepared_tables = list(table_names)

    def prepare_engines(self, engine_names):
        self.prepared_engines = list(engine_names)

    def unsubscribeTable(self, tableName, actionName):
        pass

    def dropStreamEngine(self, engineName):
        pass

    def runScript(self, script, *args, **kwargs):
        text = script.strip().rstrip(";").strip()

        # 1) 裸表名：runScript('shareTickTable')
        if text in self.tables:
            return self.tables[text].copy()

        # 2) exec count(*) from T
        match = re.fullmatch(r"exec\s+count\(\*\)\s+from\s+(\w+)", text, re.IGNORECASE)
        if match:
            return 0 if match.group(1) not in self.tables else len(self.tables[match.group(1)])

        # 2b) exec max(col) from T
        match = re.fullmatch(r"exec\s+max\((\w+)\)\s+from\s+(\w+)", text, re.IGNORECASE)
        if match:
            column, table = match.groups()
            if table not in self.tables or self.tables[table].empty:
                return None
            return self.tables[table][column].max()

        # 3) exec col from T order by C asc|desc limit N
        match = re.fullmatch(
            r"exec\s+(\w+)\s+from\s+(\w+)\s+order by\s+(\w+)\s+(asc|desc)\s+limit\s+(\d+)",
            text, re.IGNORECASE,
        )
        if match:
            column, table, order_column, direction, limit = match.groups()
            frame = self._table(table)
            frame = frame.sort_values(order_column, ascending=direction.lower() == "asc")
            return list(frame.head(int(limit))[column])

        # 4) K 线指标窗口：with temp as (... context by ... csort ... desc limit N) ...
        match = re.fullmatch(
            r"with temp as \(select \* from (\w+) where (\w+)='([^']*)' "
            r"context by (\w+) csort (\w+) desc limit (\d+)\) "
            r"select \* from temp order by (\w+)",
            text, re.IGNORECASE,
        )
        if match:
            table, filter_col, filter_val, group_col, sort_col, limit, order_col = match.groups()
            frame = self._table(table)
            frame = frame[frame[filter_col] == filter_val]
            frame = frame.sort_values(sort_col, ascending=False)
            frame = frame.groupby(group_col, sort=False).head(int(limit))
            return frame.sort_values(order_col).reset_index(drop=True)

        # 4b) select top N ... where ... [and ...] context by G csort T desc
        match = re.fullmatch(
            r"select\s+top\s+(\d+)\s+(\*|[\w, ]+?)\s+from\s+(\w+)"
            r"\s+where\s+(\w+)='([^']*)'(?:\s+and\s+(\w+)='([^']*)')?"
            r"\s+context by\s+(\w+)\s+csort\s+(\w+)\s+desc",
            text, re.IGNORECASE,
        )
        if match:
            (limit, columns, table, wcol1, wval1, wcol2, wval2,
             group_col, sort_col) = match.groups()
            frame = self._table(table)
            frame = frame[frame[wcol1] == wval1]
            if wcol2:
                frame = frame[frame[wcol2] == wval2]
            frame = frame.sort_values(sort_col, ascending=False)
            frame = frame.groupby(group_col, sort=False).head(int(limit))
            frame = frame.sort_values(sort_col).reset_index(drop=True)
            if columns.strip() != "*":
                selected = [c.strip() for c in columns.split(",") if c.strip()]
                frame = frame[selected]
            return frame

        # 5) select top N cols from T [where c1='v1' (and c2='v2')] [order by C asc|desc]
        match = re.fullmatch(
            r"select\s+top\s+(\d+)\s+([\w, ]+?)\s+from\s+(\w+)"
            r"(?:\s+where\s+(\w+)='([^']*)'(?:\s+and\s+(\w+)='([^']*)')?)?"
            r"(?:\s+order by\s+(\w+)\s+(asc|desc))?",
            text, re.IGNORECASE,
        )
        if match:
            (limit, columns, table, wcol1, wval1, wcol2, wval2,
             order_col, direction) = match.groups()
            frame = self._table(table)
            if wcol1:
                frame = frame[frame[wcol1] == wval1]
            if wcol2:
                frame = frame[frame[wcol2] == wval2]
            if order_col:
                frame = frame.sort_values(order_col, ascending=direction.lower() == "asc")
            columns = [c.strip() for c in columns.split(",") if c.strip()]
            return frame[columns].head(int(limit)).reset_index(drop=True)

        # 6) select * from T [where c1='v1' (and c2='v2')]
        match = re.fullmatch(
            r"select\s+\*\s+from\s+(\w+)"
            r"(?:\s+where\s+(\w+)='([^']*)'(?:\s+and\s+(\w+)='([^']*)')?)?",
            text, re.IGNORECASE,
        )
        if match:
            table, wcol1, wval1, wcol2, wval2 = match.groups()
            frame = self._table(table)
            if wcol1:
                frame = frame[frame[wcol1] == wval1]
            if wcol2:
                frame = frame[frame[wcol2] == wval2]
            return frame.reset_index(drop=True)

        raise NotImplementedError(f"FakeDolphin 暂不支持的查询: {text}")

    def _table(self, name: str) -> pd.DataFrame:
        if name not in self.tables:
            raise KeyError(f"FakeDolphin 未加载表 {name}（先 update() 写入）")
        return self.tables[name]


class FakeTradeApi:
    """记录下单/平仓/批量/撤单调用并返回成功应答。"""

    def __init__(self):
        self.orders = []
        self.closes = []
        self.batches = []
        self.cancels = []

    def place_order(self, **kwargs):
        self.orders.append(dict(kwargs))
        return {"code": "0", "msg": "", "data": [
            {"ordId": str(len(self.orders)), "clOrdId": kwargs.get("clOrdId", "")},
        ]}

    def place_multiple_orders(self, payload):
        self.batches.append([dict(item) for item in payload])
        return {"code": "0", "msg": "", "data": [{} for _ in payload]}

    def close_positions(self, instId, mgnMode, posSide="", ccy="", **kwargs):
        self.closes.append(
            {"instId": instId, "mgnMode": mgnMode, "posSide": posSide, "ccy": ccy}
        )
        return {"code": "0", "msg": "", "data": []}

    def cancel_order(self, instId, ordId="", clOrdId=""):
        self.cancels.append({"instId": instId, "ordId": ordId, "clOrdId": clOrdId})
        return {"code": "0", "msg": "", "data": [{}]}


class FakeApi:
    """离线替代 Kernel.api：行情规格由 instruments 提供，交易调用仅记录。"""

    def __init__(self, instruments=None):
        self.instruments = dict(instruments or {})
        self.public = SimpleNamespace(get_instruments=self._get_instruments)
        self.trade = FakeTradeApi()
        self.account = SimpleNamespace(set_position_mode=self._noop)

    def _get_instruments(self, instType, uly="", instId="", instFamily=""):
        data = [dict(self.instruments[instId])] if instId in self.instruments else []
        return {"code": "0", "msg": "", "data": data}

    @staticmethod
    def _noop(*args, **kwargs):
        return {"code": "0", "msg": "", "data": []}

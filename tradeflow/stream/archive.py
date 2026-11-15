"""流表 -> DFS 历史归档（P1）。

归档表只存策略回测需要的核心列，避免重复存全量 63 列订单。
写法细节（均在 DolphinDB 3.00.6 端到端实测验证，勿再"优化"）：

1. COMPO 分区至少需要两个子域（VALUE(date) + HASH(instId)），单个子域报错；
2. 必须用函数式 ``createPartitionedTable(db, t, ...)``——
   ``db.createPartitionedTable(...)`` 作为独立语句在该版本解析失败；
3. handler 函数体内不能引用全局变量（如 dbPath），路径必须写字面量；
4. DFS 分区表第一列是 date(ts)，流表没有 date 列，因此不能用
   ``append!{loadTable(...)}`` 直接追加，必须先 ``select date(ts) as date``
   转换再 ``tableInsert``；
5. 查询归档表必须带 date 过滤，否则分区数过多直接报错；
6. handler 定义与引用它的 subscribeTable 放在同一个脚本块内。
"""
from __future__ import annotations

ARCHIVE_DB = "dfs://market"

ARCHIVE_DB_SCRIPT = """
dbDate = database("", VALUE, 2024.01.01..2030.12.31)
dbHash = database("", HASH, [SYMBOL, 30])
db = database("dfs://market", COMPO, [dbDate, dbHash])
if (!existsTable("dfs://market", `tick)) {
    t = table(1:0, `date`instId`ts`last`lastSz, [DATE,SYMBOL,TIMESTAMP,DOUBLE,DOUBLE])
    createPartitionedTable(db, t, `tick, `date`instId)
}
if (!existsTable("dfs://market", `trade)) {
    t = table(1:0, `date`instId`ts`px`sz, [DATE,SYMBOL,TIMESTAMP,DOUBLE,DOUBLE])
    createPartitionedTable(db, t, `trade, `date`instId)
}
"""

# (流表, action, DFS 表名, select 列，顺序与 DFS 表列一致)
ARCHIVE_RULES = (
    ("tickStreamTable", "archiveTick", "tick", "instId, ts, last, lastSz"),
    ("tradeStreamTable", "archiveTrade", "trade", "instId, ts, px, sz"),
)


def ensure_archive(dolphin=None) -> None:
    """创建归档库表并订阅流表写入 DFS（幂等：重复调用重建订阅）。"""
    from .dolphin import Dolphin

    d = dolphin or Dolphin()
    d.runScript(ARCHIVE_DB_SCRIPT)
    for table, action, target, cols in ARCHIVE_RULES:
        # handler 定义与 subscribeTable 必须在同一个脚本块内，
        # 且函数体只能用路径字面量，不能引用全局变量。
        d.runScript(f"""
        def {action}(msg) {{
            tableInsert(loadTable("{ARCHIVE_DB}", `{target}), select date(ts) as date, {cols} from msg)
        }}
        try {{ unsubscribeTable(tableName=`{table}, actionName="{action}") }} catch(ex) {{}}
        subscribeTable(
            tableName=`{table},
            actionName="{action}",
            offset=0,
            handler={action},
            msgAsTable=true,
            batchSize=10000,
            throttle=5,
            persistOffset=true
        )
        """)
        print(f"archived {table} -> {ARCHIVE_DB}/{target}!")

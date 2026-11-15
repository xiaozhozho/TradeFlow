"""DolphinDB 流计算引擎定义与创建脚本。

引擎矩阵：
- ``subscription``: 流表 -> latestIndexedTable 快照（tableInsert 直写）
- ``reactive``:     createReactiveStateEngine，K 线指标（只喂 confirm=1）
- ``timeseries``:   createTimeSeriesEngine，tick 5 分钟窗口活跃度

输出表列顺序（实测验证，勿改）：
- reactive: 分组列(keyColumn) -> metrics
- timeseries: 时间列 -> 分组列(keyColumn) -> metrics
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class EngineSpec:
    name: str
    source: str
    action: str
    target: str
    kind: str  # subscription / reactive / timeseries
    metrics: str = ""
    key_columns: tuple[str, ...] = ()
    time_column: str = "ts"
    window_size: int = 0
    step: int = 0


ENGINE_SPECS = {
    "lastTickAction": EngineSpec(
        "lastTickAction", "tickStreamTable", "lastTickAction",
        "shareTickTable", "subscription",
    ),
    "lastCandleAction": EngineSpec(
        "lastCandleAction", "candleStreamTable", "lastCandleAction",
        "shareCandleTable", "subscription",
    ),
    "lastOrderAction": EngineSpec(
        "lastOrderAction", "orderStreamTable", "lastOrderAction",
        "shareOrderTable", "subscription",
    ),
    "lastAlgoOrderAction": EngineSpec(
        "lastAlgoOrderAction", "algoOrderStreamTable", "lastAlgoOrderAction",
        "shareAlgoOrderTable", "subscription",
    ),
    "lastAccountGreeksAction": EngineSpec(
        "lastAccountGreeksAction", "accountGreeksStreamTable", "lastAccountGreeksAction",
        "shareAccountGreeksTable", "subscription",
    ),
    # 指标输出表的最新快照（指标引擎输出 -> latestIndexedTable）
    "lastCandleIndicatorAction": EngineSpec(
        "lastCandleIndicatorAction", "candleIndicatorStreamTable", "lastCandleIndicatorAction",
        "shareCandleIndicatorTable", "subscription",
    ),
    "lastTickIndicatorAction": EngineSpec(
        "lastTickIndicatorAction", "tickIndicatorStreamTable", "lastTickIndicatorAction",
        "shareTickIndicatorTable", "subscription",
    ),
    "candleIndicatorEngine": EngineSpec(
        "candleIndicatorEngine", "candleConfirmStreamTable", "candleIndicatorAction",
        "candleIndicatorStreamTable", "reactive",
        metrics="""<[
        ts,
        mavg(c, 5, 1) as ma5,
        mavg(c, 10, 1) as ma10,
        mavg(c, 20, 1) as ma20,
        ema(c, 12) as ema12,
        ema(c, 26) as ema26,
        ema(c, 12) - ema(c, 26) as macd,
        mstd(c, 20, 1) as mstd20,
        mavg(vol, 5, 1) as volMa5,
        mmax(h, 20, 1) as high20,
        mmin(l, 20, 1) as low20
    ]>""",
        key_columns=("instId", "channel"),
    ),
    "tickIndicatorEngine": EngineSpec(
        "tickIndicatorEngine", "tickStreamTable", "tickIndicatorAction",
        "tickIndicatorStreamTable", "timeseries",
        metrics="""<[
        count(*) as count5m,
        first(last) as open5m,
        max(last) as high5m,
        min(last) as low5m,
        last(last) as close5m,
        avg(last) as avgPx,
        sum(lastSz) as vol5m
    ]>""",
        key_columns=("instId",),
        window_size=300000,
        step=60000,
    ),
}

ENGINE_NAMES = tuple(ENGINE_SPECS)


def create_engine_script(spec: EngineSpec) -> str:
    """生成单个引擎的重建脚本（先摘除旧订阅/引擎，再创建）。"""
    unsubscribe = (
        f'try {{ unsubscribeTable(tableName=`{spec.source}, '
        f'actionName="{spec.action}") }} catch(ex) {{}}'
    )
    keys = "`" + "`".join(spec.key_columns) if spec.key_columns else ""
    if spec.kind == "subscription":
        return f"""
{unsubscribe}
subscribeTable(
    tableName=`{spec.source},
    actionName="{spec.action}",
    offset=0,
    handler=tableInsert{{{spec.target}}},
    msgAsTable=true
)
"""
    if spec.kind == "reactive":
        return f"""
{unsubscribe}
try {{ dropStreamEngine(`{spec.name}) }} catch(ex) {{}}
{spec.name} = createReactiveStateEngine(
    name="{spec.name}",
    metrics={spec.metrics},
    dummyTable={spec.source},
    outputTable={spec.target},
    keyColumn={keys}
)
subscribeTable(
    tableName=`{spec.source},
    actionName="{spec.action}",
    offset=0,
    handler=tableInsert{{{spec.name}}},
    msgAsTable=true
)
"""
    return f"""
{unsubscribe}
try {{ dropStreamEngine(`{spec.name}) }} catch(ex) {{}}
{spec.name} = createTimeSeriesEngine(
    name="{spec.name}",
    windowSize={spec.window_size},
    step={spec.step},
    metrics={spec.metrics},
    dummyTable={spec.source},
    outputTable={spec.target},
    timeColumn=`{spec.time_column},
    useSystemTime=false,
    keyColumn={keys},
    useWindowStartTime=false,
    fill="none"
)
subscribeTable(
    tableName=`{spec.source},
    actionName="{spec.action}",
    offset=0,
    handler=tableInsert{{{spec.name}}},
    msgAsTable=true
)
"""

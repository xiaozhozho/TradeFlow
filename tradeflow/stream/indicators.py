"""策略自定义指标：声明式生成 DolphinDB 指标表与流引擎。

用法（Strategy 类属性）::

    class MyStrategy(Strategy):
        candles = ("candle1m", "candle15m")
        indicators = {"ma": [5, 15], "rsi": [14], "boll": [(20, 2)], "atr": [14]}

框架在 ``Kernel.prepare()`` 时按声明自动生成：

- 流表 ``indicator_<Cls>StreamTable``（instId/channel/ts + 指标列）；
- 快照表 ``share<Cls>IndicatorTable``（latestIndexedTable，按 instId+channel）；
- reactive state engine ``<Cls>IndicatorEngine``（源：candleConfirmStreamTable，
  一次覆盖该策略全部周期）+ 快照订阅 ``<Cls>IndicatorAction``。

指标表达式只用 DolphinDB 内置滚动函数（mavg/mstd/ema/mmax/mmin/prev），
不依赖 ta 扩展。已在 3.00.6 实测：``ta::rsi/atr/boll`` 不存在；
``prev()`` 在 reactive state engine 中可用（RSI/ATR 按此实现）。
"""
from __future__ import annotations

from dataclasses import dataclass

from .engines import ENGINE_SPECS, EngineSpec
from .schema import Column, STREAM_TABLES, TableSpec

# 指标名 -> (表达式模板, 参数个数)
_TEMPLATES = {
    "ma": ("mavg(c, {0}, 1)", 1),
    "ema": ("ema(c, {0})", 1),
    "mstd": ("mstd(c, {0}, 1)", 1),
    "high": ("mmax(h, {0}, 1)", 1),
    "low": ("mmin(l, {0}, 1)", 1),
    "volma": ("mavg(vol, {0}, 1)", 1),
    "macd": ("ema(c, {0}) - ema(c, {1})", 2),
    "rsi": (
        "100.0 - 100.0 / (1.0 + "
        "mavg(max(c - prev(c), 0.0), {0}, 1) / mavg(max(prev(c) - c, 0.0), {0}, 1))",
        1,
    ),
    "atr": ("mavg(max(h - l, max(abs(h - prev(c)), abs(l - prev(c)))), {0}, 1)", 1),
}

SUPPORTED = tuple(sorted(_TEMPLATES)) + ("boll",)


def _format_param(value) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return str(int(number)) if number.is_integer() else str(number)


def declaration_to_metrics(declaration: dict) -> list[tuple[str, str]]:
    """{指标名: [参数...]} -> [(列名, DDB 表达式)]；boll 展开为上/中/下三列。"""
    metrics: list[tuple[str, str]] = []
    for name, params in (declaration or {}).items():
        key = str(name).lower()
        for param in params or []:
            values = tuple(param) if isinstance(param, (tuple, list)) else (param,)
            if key == "boll":
                n = int(values[0])
                k = _format_param(values[1]) if len(values) > 1 else "2"
                mid = f"mavg(c, {n}, 1)"
                std = f"mstd(c, {n}, 1)"
                prefix = f"boll_{n}_{k}"
                metrics.extend([
                    (f"{prefix}_up", f"{mid} + {k} * {std}"),
                    (f"{prefix}_mid", mid),
                    (f"{prefix}_dn", f"{mid} - {k} * {std}"),
                ])
                continue
            template = _TEMPLATES.get(key)
            if template is None:
                raise ValueError(f"未支持的指标 {name!r}，可用: {list(SUPPORTED)}")
            expression, arity = template
            if len(values) != arity:
                raise ValueError(f"指标 {name} 需要 {arity} 个参数，收到 {values}")
            column = f"{key}_" + "_".join(_format_param(value) for value in values)
            metrics.append((column, expression.format(*values)))
    if not metrics:
        raise ValueError("指标声明为空")
    return metrics


@dataclass(frozen=True)
class IndicatorPlan:
    """一个策略的指标表/引擎计划。"""

    stream_table: TableSpec
    share_table: TableSpec
    engine: EngineSpec
    action: EngineSpec

    @property
    def table_names(self) -> set[str]:
        return {self.stream_table.name, self.share_table.name}

    @property
    def engine_names(self) -> tuple[str, ...]:
        return (self.engine.name, self.action.name)


def build_indicator_plan(class_name: str, declaration: dict) -> IndicatorPlan:
    metrics = declaration_to_metrics(declaration)
    stream_name = f"indicator_{class_name}StreamTable"
    share_name = f"share{class_name}IndicatorTable"
    engine_name = f"{class_name}IndicatorEngine"
    action_name = f"{class_name}IndicatorAction"

    # 列顺序必须与引擎输出一致：key 列 -> ts -> metrics
    columns = (
        Column("instId", "SYMBOL", "str"),
        Column("channel", "SYMBOL", "str"),
        Column("ts", "TIMESTAMP", "ts"),
    ) + tuple(Column(column, "DOUBLE", "float") for column, _ in metrics)

    stream_table = TableSpec(
        stream_name, columns=columns, capacity=200_000, retention_minutes=180,
        description=f"{class_name} 自定义指标流表",
    )
    share_table = TableSpec(
        share_name, columns=columns, key_columns=("instId", "channel"),
        time_column="ts", capacity=5000, latest=True,
        description=f"{class_name} 最新自定义指标",
    )
    metric_expr = (
        "<[" + ", ".join(["ts"] + [f"{expr} as {column}" for column, expr in metrics]) + "]>"
    )
    engine = EngineSpec(
        engine_name, "candleConfirmStreamTable", action_name, stream_name,
        "reactive", metrics=metric_expr, key_columns=("instId", "channel"),
    )
    action = EngineSpec(action_name, stream_name, action_name, share_name, "subscription")
    return IndicatorPlan(stream_table, share_table, engine, action)


def register_strategy_indicators(strategies) -> tuple[set[str], tuple[str, ...]]:
    """按策略声明注册动态表与引擎（幂等），返回 (表名集合, 引擎名元组)。"""
    tables: set[str] = set()
    engines: list[str] = []
    for strategy in strategies:
        declaration = getattr(strategy, "indicators", None)
        if not declaration:
            continue
        plan = build_indicator_plan(strategy.__class__.__name__, declaration)
        for spec in (plan.stream_table, plan.share_table):
            STREAM_TABLES[spec.name] = spec
            tables.add(spec.name)
        for spec in (plan.engine, plan.action):
            ENGINE_SPECS[spec.name] = spec
            if spec.name not in engines:
                engines.append(spec.name)
    return tables, tuple(engines)

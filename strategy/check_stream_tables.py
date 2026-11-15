"""检查流数据表订阅写入情况。

启动 OKX WebSocket 订阅（Kernel 完整管道），轮询 DolphinDB 中每张
流数据表/快照表的数据量，验证"订阅成功 -> 解析成功 -> 写库成功"全链路。

运行方式（在项目根目录）：

    python -m strategy.check_stream_tables                      # 默认公共行情，90 秒
    python -m strategy.check_stream_tables --timeout 120        # 观察 2 分钟
    python -m strategy.check_stream_tables --depth books5       # 开启盘口后检查
    python -m strategy.check_stream_tables --private            # 连同私有频道一起检查

判定规则（避免误报）：
- ✓ 有数据：本次窗口内行数 > 0；
- ✗ 异常：预期持续有数据的表始终为 0（订阅/解析/写库某环失败）；
- ○ 未启用：表未创建（对应频道未开启，如 depth/algo/fills）；
- - 等待事件：稀有事件表（status/爆仓单/产品变更/订单变动等）短时间内
  没有推送属正常，不算失败。

进程退出码：出现 ✗ 返回 1，否则 0。Ctrl+C 提前结束并出最终报告。
"""
import argparse
import sys
import time

sys.path.append("..")  # 将 tradeflow 包所在目录加入 Python 模块搜索路径

from tradeflow import Kernel
from tradeflow.stream.schema import STREAM_TABLES
from tradeflow.subscribe.channels import CHANNEL_SPECS
from tradeflow.utils.log import get_logger

logger = get_logger()

# 频道清单唯一来源 -> 表 -> 写入它的频道（用于区分"频道未订阅"与"订阅了但无推送"）
TABLE_CHANNELS = {}
for _channel_name, _spec in CHANNEL_SPECS.items():
    for _table in _spec.tables:
        TABLE_CHANNELS.setdefault(_table, []).append(_channel_name)

# 预期窗口内持续有数据的表（默认公共频道 + 引擎输出）
ACTIVE_TABLES = {
    "tickStreamTable", "tradeStreamTable",
    "candleStreamTable", "candleConfirmStreamTable",
    "fundingrateStreamTable", "markpriceStreamTable",
    "indextickStreamTable", "openinterestStreamTable",
    "candleIndicatorStreamTable", "tickIndicatorStreamTable",
    "shareTickTable", "shareCandleTable",
    "shareCandleIndicatorTable", "shareTickIndicatorTable",
}

# 条件表：对应频道开启后才写入（稀有推送不算失败）
CONDITIONAL_TABLES = {
    "bookStreamTable": "depth 参数（books5/bbo-tbt/books）",
    "algoOrderStreamTable": "algo=True 且有策略委托触发",
    "shareAlgoOrderTable": "algo=True 且有策略委托触发",
    "fillsStreamTable": "fills 频道（需 VIP4+，默认关闭）",
    "orderStreamTable": "private=True 且有订单变动",
    "shareOrderTable": "private=True 且有订单变动",
    "shareAccountTable": "private=True（登录后推送）",
    "sharePositionTable": "private=True 且有持仓变动",
    "shareBalanceTable": "private=True（订阅后推送）",
    "shareWarningTable": "private=True 且触发爆仓预警（稀有）",
    "accountGreeksStreamTable": "private=True 且账户 Greeks 变动",
    "shareAccountGreeksTable": "private=True 且账户 Greeks 变动",
}

# 稀有公共事件表：行情平稳时无推送属正常
PASSIVE_TABLES = {
    "statusStreamTable": "系统维护状态事件",
    "liquidationorderStreamTable": "爆仓单事件",
    "productStreamTable": "instruments 频道仅在产品信息变更时推送",
}


def query_counts(dolphin, tables):
    """逐表查询行数；表未创建返回 None。"""
    counts = {}
    for name in tables:
        try:
            counts[name] = int(dolphin.runScript(f"exec count(*) from {name}"))
        except Exception:
            counts[name] = None  # 表不存在（对应频道未启用）
    return counts


def render(counts, prev, elapsed):
    """渲染一行式报告。"""
    print(f"\n===== 流数据表数据量（已运行 {elapsed:.0f}s） =====")
    print(f"{'表名':<32}{'行数':>10}{'增量':>8}  状态")
    for name in sorted(counts):
        rows = counts[name]
        if rows is None:
            print(f"{name:<32}{'-':>10}{'-':>8}  ○ 未创建（频道未启用）")
            continue
        delta = "" if prev is None else f"+{rows - prev[name]}" if rows > prev[name] else "0"
        print(f"{name:<32}{rows:>10}{delta:>8}")


def classify(name, rows, enabled_conditions, enabled_channels=frozenset()):
    """返回 (符号, 说明, 是否致命)。"""
    if rows is None:
        return "○", f"未创建（条件：{CONDITIONAL_TABLES.get(name, '未启用频道')}）", False
    if rows > 0:
        return "✓", "", False
    if name in ACTIVE_TABLES:
        return "✗", "预期持续有数据但为 0，检查订阅/写库", True
    # 表存在但写入它的频道一个都没订阅 -> 未启用
    channels = TABLE_CHANNELS.get(name) or []
    if channels and not (set(channels) & set(enabled_channels)):
        return "○", f"表已创建但频道未订阅（{'/'.join(sorted(channels))}）", False
    if name in CONDITIONAL_TABLES:
        if enabled_conditions.get(name):
            return "-", f"已启用但窗口内无推送（{CONDITIONAL_TABLES[name]}），非致命", False
        return "○", f"未启用（条件：{CONDITIONAL_TABLES[name]}）", False
    if name in PASSIVE_TABLES:
        return "-", f"稀有事件，窗口内无推送属正常（{PASSIVE_TABLES[name]}）", False
    return "-", "", False


def main():
    parser = argparse.ArgumentParser(description="检查流数据表订阅写入情况")
    parser.add_argument("--timeout", type=int, default=90, help="观察时长（秒），默认 90")
    parser.add_argument("--interval", type=int, default=5, help="轮询间隔（秒），默认 5")
    parser.add_argument("--instIds", default="BTC-USDT-SWAP,ETH-USDT-SWAP",
                        help="逗号分隔的合约列表")
    parser.add_argument("--flag", default="1", choices=("0", "1"), help="'1' 模拟盘 / '0' 实盘")
    parser.add_argument("--private", action="store_true", help="同时订阅私有频道（需密钥）")
    parser.add_argument("--depth", default=None, choices=("books5", "bbo-tbt", "books"))
    parser.add_argument("--options", action="store_true", help="订阅期权频道")
    parser.add_argument("--algo", action="store_true", help="订阅策略委托频道")
    args = parser.parse_args()

    instIds = {s.strip() for s in args.instIds.split(",") if s.strip()}
    kernel = Kernel(
        flag=args.flag, pause=1, warmup=0, private=args.private,
        instIds=instIds, depth=args.depth, options=args.options, algo=args.algo,
    )

    print(f"== 准备流数据表与引擎（instIds={sorted(instIds)}） ==")
    kernel.prepare()
    print("== 启动 WebSocket 订阅 ==")
    kernel.subscribe()

    enabled_conditions = {
        "bookStreamTable": args.depth is not None,
        "algoOrderStreamTable": args.algo,
        "shareAlgoOrderTable": args.algo,
        "fillsStreamTable": False,  # fills 默认不订阅（VIP4+）
    }
    tables = set(STREAM_TABLES)

    prev = None
    fatal = []
    deadline = time.monotonic() + args.timeout
    try:
        while time.monotonic() < deadline:
            time.sleep(args.interval)
            counts = query_counts(kernel.dolphin, tables)
            render(counts, prev, args.timeout - (deadline - time.monotonic()))
            prev = counts
    except KeyboardInterrupt:
        print("\n收到退出信号，提前结束观察")
    finally:
        # 最终判定
        counts = prev if prev is not None else query_counts(kernel.dolphin, tables)
        print(f"\n===== 最终判定（观察 {args.timeout}s，频道条件：depth={args.depth} "
              f"options={args.options} algo={args.algo} private={args.private}） =====")
        for name in sorted(counts):
            mark, reason, is_fatal = classify(
                name, counts[name], enabled_conditions, kernel.enabled_channels(),
            )
            if is_fatal:
                fatal.append(name)
            line = f"{mark} {name:<30}"
            if reason:
                line += f"  {reason}"
            print(line)

        health = kernel.health()
        print("\n===== 订阅健康 =====")
        for endpoint in ("public", "business", "private", "algo"):
            info = health.get(endpoint)
            if info is None:
                continue
            print(f"{endpoint}: connected={info['connected']} "
                  f"errors={info['error_count']} reconnects={info['reconnect_count']} "
                  f"channels={len(info['channels'])}")
        print(f"writer: {health['writer']}")

        if fatal:
            print(f"\n结果: 失败 —— {len(fatal)} 张预期有数据的表为空: {fatal}")
            code = 1
        else:
            print("\n结果: 通过 —— 预期有数据的表均有数据写入")
            code = 0
        kernel.stop()
        sys.exit(code)


if __name__ == "__main__":
    main()

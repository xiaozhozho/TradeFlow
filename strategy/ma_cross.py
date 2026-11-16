"""MA5/MA15 均线交叉模拟盘策略（多合约，1 分钟 K 线）。

规则：
- 同时交易多个 U 本位永续（``INST_IDS`` 默认 10 个：
  BTC/ETH/SOL/DOGE/XRP/OKB/LTC/PEPE/XAU/BZ-USDT-SWAP），
  每个合约独立判断信号、独立下单（互不影响）；
- 每个合约单笔保证金固定 ``MARGIN_USDT``（默认 10 USDT，按持仓合约数累加，
  如 10 个合约全部持仓时占用保证金约 100 USDT）；
- 指标声明式：``indicators = {"ma": [5, 15]}`` 由框架自动在
  candleConfirmStreamTable 上计算（DolphinDB reactive engine，
  见 tradeflow/stream/indicators.py），策略无需自己算均线；
- MA5 上穿 MA15（金叉）做多；下穿（死叉）平多并反向做空
  （净持仓模式，Kernel.init 已设置；反手时先市价平仓再开反向仓）；
- 同一合约同一根 K 线上的交叉只执行一次，避免逐秒重复下单；
- 每轮 scan() 先打印 tickStreamTable 当前数据量，便于观察行情落库是否正常
  （查询失败只记日志，不影响交易逻辑）；
- 单个合约下单失败（保证金不足/限频/限额等）不影响其他合约。

数量计算：``buyByMargin`` 由 kernel.trader 按
保证金 x 杠杆 / (价格 x 合约面值 ctVal) 换算，并按 lotSz/minSz 规整；
杠杆取 ``kernel.risk.lever``（默认 settings.LEVER=30）。

运行（项目根目录，需在 .env 配置模拟盘密钥 OKX_*_SIMULATE）：

    python -m strategy.ma_cross

日志写入 strategy/log/ma_cross.log，Ctrl+C 停止。
"""
import math
import sys

sys.path.append("..")  # noqa: E402

from tradeflow import Kernel
from tradeflow.strategy import Strategy

INST_IDS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP",
    "XRP-USDT-SWAP",
    'OKB-USDT-SWAP',
    'LTC-USDT-SWAP',
    'PEPE-USDT-SWAP',
    'XAU-USDT-SWAP',
    'BZ-USDT-SWAP',
)
FAST_MA = 5
SLOW_MA = 15


def cross_signal(prev_fast, prev_slow, cur_fast, cur_slow) -> str | None:
    """两根 K 线的 MA 值判定交叉：返回 'long' / 'short' / None。

    上一根 fast<=slow 且当前 fast>slow 为金叉（long），反向为死叉（short）；
    指标未成形（NaN/None）时返回 None。
    """
    values = (prev_fast, prev_slow, cur_fast, cur_slow)
    for value in values:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
    prev_diff = float(prev_fast) - float(prev_slow)
    cur_diff = float(cur_fast) - float(cur_slow)
    if prev_diff <= 0 < cur_diff:
        return "long"
    if prev_diff >= 0 > cur_diff:
        return "short"
    return None


class MaCrossStrategy(Strategy):
    """MA5/MA15 均线交叉模拟盘策略（多合约独立信号）。"""

    instIds = set(INST_IDS)
    candles = ("candle1m",)
    indicators = {"ma": [FAST_MA, SLOW_MA]}
    MARGIN_USDT = 10.0  # 每个合约单笔保证金（USDT）

    def __init__(self, kernel):
        super().__init__(kernel)
        self._last_signal_ts = {}  # {instId: bar_ts}：按合约各自去重

    def preinit(self):
        self.log(
            f"preinit: MA{FAST_MA}/MA{SLOW_MA} 交叉策略，"
            f"{len(INST_IDS)} 个合约，每个保证金 {self.MARGIN_USDT:g} USDT"
        )

    def init(self):
        self.log(
            f"init: flag={self.kernel.flag}（'1' 模拟盘）, "
            f"lever={self.kernel.risk.lever}, "
            f"instIds={sorted(self.instIds)}, "
            f"指标=indicator_MaCrossStrategyStreamTable/candle1m"
        )

    # ---------- 交易逻辑 ----------

    def _log_tick_count(self) -> None:
        """每轮 scan 打印 tickStreamTable 当前数据量（查询失败不影响交易逻辑）。"""
        try:
            count = int(self.kernel.dolphin.runScript("exec count(*) from tickStreamTable"))
        except Exception as exc:
            self.log(f"tickStreamTable 数据量查询失败: {exc}")
            return
        self.log(f"tickStreamTable 数据量: {count}")

    def scan(self):
        self._log_tick_count()

        indicators = self.getIndicators("candle1m", limit=3)
        if indicators.empty:
            return
        for inst_id, group in indicators.groupby("instId", sort=False):
            if len(group) < 2:
                continue  # 该合约指标未就绪（刚启动/断线补齐中），等待
            group = group.sort_values("ts")
            prev_row = group.iloc[-2]
            cur_row = group.iloc[-1]
            signal = cross_signal(
                prev_row[f"ma_{FAST_MA}"], prev_row[f"ma_{SLOW_MA}"],
                cur_row[f"ma_{FAST_MA}"], cur_row[f"ma_{SLOW_MA}"],
            )
            if signal is None:
                continue
            try:
                self._handle_signal(inst_id, signal, str(cur_row["ts"]))
            except Exception as exc:  # 单合约失败不影响其他合约
                self.log(f"{inst_id} {signal} 信号执行失败：{exc}")

    def _handle_signal(self, inst_id, signal, bar_ts):
        """处理单合约信号：去重 -> 同向跳过 -> 反手平仓 -> 按保证金下单。"""
        if self._last_signal_ts.get(inst_id) == bar_ts:
            return  # 同一合约同一根 K 线的交叉只处理一次
        self._last_signal_ts[inst_id] = bar_ts

        position = self.getPosition(inst_id)
        pos = float(position.get("pos") or 0) if position else 0.0
        if (signal == "long" and pos > 0) or (signal == "short" and pos < 0):
            return  # 已与信号同向，不重复开仓

        if pos != 0:
            self.close(inst_id)
            self.log(f"{inst_id} {signal} 信号：先平掉现有仓位 pos={pos:g}")
        packet = (
            self.buyByMargin(inst_id, self.MARGIN_USDT)
            if signal == "long"
            else self.sellByMargin(inst_id, self.MARGIN_USDT)
        )
        action = "买入做多" if signal == "long" else "卖出做空"
        self.log(
            f"{inst_id} {signal} 信号：{action}（保证金 {self.MARGIN_USDT:g}U x "
            f"{self.kernel.risk.lever} 倍） -> code={packet.get('code')} "
            f"msg={packet.get('msg', '')}"
        )

    # ---------- 清理（框架预留钩子） ----------

    def unsubscribeTable(self):
        """取消策略相关的流表订阅。"""
        pass

    def dropStreamTable(self):
        """删除策略相关的流数据表。"""
        pass

    def dropStreamEngine(self):
        """删除策略相关的流数据引擎。"""
        pass


def main():
    # 模拟盘：flag='1' 使用 .env 中 OKX_*_SIMULATE 密钥；交易 INST_IDS 中的 U 本位永续
    kernel = Kernel(flag="1", pause=1, warmup=5, private=True)
    kernel.addStrategy(MaCrossStrategy)
    try:
        kernel.start()
    except KeyboardInterrupt:
        kernel.log("收到退出信号，正在停止...")
    finally:
        kernel.stop()


if __name__ == "__main__":
    main()

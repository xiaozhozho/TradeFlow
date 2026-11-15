"""量化策略示例：OKX 行情落库 + DolphinDB 流表统计。

运行方式（在项目根目录）：

    python -m strategy.example

执行流程：
1. `Kernel.prepare()` 创建 DolphinDB 流数据表（tickStreamTable、candleStreamTable、
   orderStreamTable 等）、共享内存表以及 `lastTickAction` 流数据订阅引擎；
2. `Kernel.subscribe()` 启动 OKX WebSocket 公共频道订阅（tickers + 1m/5m K 线），
   tick 行情写入 `tickStreamTable`；本示例 `private=False`，无需私有 API Key；
3. 策略在 `scan()` 中每 3 秒统计一次 `tickStreamTable` 的数据量并输出。

日志写入 `strategy/log/example.log`，同时输出到控制台。Ctrl+C 可停止策略。
"""
import time

from tradeflow import Kernel
from tradeflow.strategy import Strategy


class ExampleStrategy(Strategy):
    """每 3 秒统计 tick 流表数据量。"""

    instIds = {"BTC-USDT-SWAP"}
    REPORT_INTERVAL = 3  # 秒

    def __init__(self, kernel):
        super().__init__(kernel)
        self._last_report = 0.0
        self._last_count = None

    def preinit(self):
        self.log("preinit: 即将创建 DolphinDB 流数据表与 lastTickAction 引擎")

    def init(self):
        self.log("init: 流数据表/引擎已就绪，OKX WebSocket 行情订阅已启动")
        self._report_tick_count()

    def scan(self):
        """Kernel 按 pause 间隔调用，这里控制为每 3 秒统计一次。"""
        if time.monotonic() - self._last_report >= self.REPORT_INTERVAL:
            self._report_tick_count()

    def _report_tick_count(self):
        self._last_report = time.monotonic()
        count = int(self.kernel.dolphin.runScript("exec count(*) from tickStreamTable"))
        if self._last_count is None:
            detail = ""
        else:
            detail = f"，较上次新增 {count - self._last_count} 条"
        self._last_count = count
        self.log(f"tickStreamTable 当前数据量: {count} 条{detail}")

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
    # flag="1" 模拟盘；本示例只消费公共行情，private=False 不订阅账户/持仓
    # pause 是 Kernel 循环间隔，warmup 是启动后等待订阅就绪的秒数
    kernel = Kernel(flag="1", pause=1, warmup=3, private=False)
    kernel.addStrategy(ExampleStrategy)
    try:
        kernel.start()
    except KeyboardInterrupt:
        kernel.log("收到退出信号，正在停止...")
    finally:
        kernel.stop()


if __name__ == "__main__":
    main()

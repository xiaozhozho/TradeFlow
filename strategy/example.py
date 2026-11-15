"""最小策略模板。

复制本文件为新的策略文件后按需修改，并在项目根目录运行：

    python -m strategy.example

策略通过 `instIds` 声明关注的交易对，Kernel 会据此订阅 tick 与 1m/5m K 线。
"""
from tradeflow import Kernel
from tradeflow.strategy import Strategy


class ExampleStrategy(Strategy):
    """示例策略：仅打印生命周期日志，不产生交易。"""

    instIds = {"BTC-USDT-SWAP"}

    def preinit(self):
        self.log("preinit")

    def init(self):
        self.log("init")

    def unsubscribeTable(self):
        """取消策略相关的流表订阅。"""
        pass

    def dropStreamTable(self):
        """删除策略相关的流数据表。"""
        pass

    def dropStreamEngine(self):
        """删除策略相关的流数据引擎。"""
        pass

    def scan(self):
        """交易逻辑入口，Kernel 会按 pause 间隔循环调用。"""
        # ticks = self.getLastTicks()
        # if ...:
        #     self.kernel.api.trade.place_order(...)
        pass


def main():
    # flag="1" 使用模拟盘配置（OKX_*_SIMULATE），"0" 使用实盘配置。
    kernel = Kernel(flag="1")
    kernel.addStrategy(ExampleStrategy)
    kernel.start()


if __name__ == "__main__":
    main()

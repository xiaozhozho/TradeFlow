import time
from datetime import datetime
from abc import abstractmethod
from .kernel import Kernel
from .indicator import Indicator


class Strategy:
    """ This strategy class """
    def __init__(self, kernel:Kernel) -> None:
        self.kernel = kernel
        self._active = False
        self.disabled = False    # 连续异常达到 kernel.max_errors 后由 Kernel 停用
        self._error_count = 0    # 连续异常次数（成功一次即清零）

    def log(self, msg:str):
        """ Log a message to the kernel"""
        self.kernel.logger.info(f'<{self.__class__.__name__}>: {msg}')
     
    @abstractmethod
    def preinit(self):
        """Preinitialize in the kernel"""
        pass

    @abstractmethod
    def init(self):
        """ Initialize the strategy """
        pass

    def getLastTicks(self):
        """获取所有最新的 tick 数据"""
        ticks = self.kernel.dolphin.runScript('shareTickTable').set_index('instId').T.to_dict()
        return ticks

    def getCandleIndicator(self, limit=60, channel='candle1m'):
        """获取K线指标（按频道过滤，默认 1 分钟）"""
        script = f"""
        with temp as (select * from candleIndicatorStreamTable where channel='{channel}' context by instId csort ts desc limit {limit}) select * from temp order by ts
        """
        data = self.kernel.dolphin.runScript(script).drop_duplicates(subset=['ts','instId'], keep='last')
        candleIndicator = Indicator(data, limit)
        return candleIndicator

    def getLastCandleIndicators(self, channel='candle1m'):
        """ 获取k线级别的最新指标 """
        lastCandleIndicator = self.kernel.dolphin.runScript(
            f"select * from shareCandleIndicatorTable where channel='{channel}'"
        ).set_index('instId').T.to_dict()
        return lastCandleIndicator
    
    def getLastTickIndicators(self):
        """获取tick级别的最新指标"""
        lastTickIndicator = self.kernel.dolphin.runScript('shareTickIndicatorTable').set_index('instId').T.to_dict()
        return lastTickIndicator

    # ***********   交易便捷方法（薄封装：kernel.trader -> okx SDK）   ***********

    def buy(self, instId, sz, px=None, **kwargs):
        """买入（默认市价）；sz/px 自动按合约规格规整，返回 OKX 原始应答。"""
        return self.kernel.trader.buy(instId, sz, px=px, **kwargs)

    def sell(self, instId, sz, px=None, **kwargs):
        """卖出（默认市价）；reduceOnly=True 表示只减仓。"""
        return self.kernel.trader.sell(instId, sz, px=px, **kwargs)

    def close(self, instId, **kwargs):
        """按仓位快照平仓（与 RiskManage 相同的 close_positions 通道）。"""
        return self.kernel.trader.close(instId, **kwargs)

    def getPosition(self, instId):
        """该品种的持仓快照（无持仓返回 None）。"""
        return self.kernel.trader.getPosition(instId)

    def getPositions(self, instId=None):
        """持仓快照列表（可按品种过滤）。"""
        return self.kernel.trader.getPositions(instId)

    def getAccount(self, ccy=None):
        """账户资金快照（ccy=None 返回全部币种）。"""
        return self.kernel.trader.getAccount(ccy)

    @abstractmethod
    def unsubscribeTable(self):
        """取消策略有关的流表订阅"""
        pass 

    @abstractmethod
    def dropStreamTable(self):
        """取消策略相关的流数据表"""
        pass

    @abstractmethod
    def dropStreamEngine(self):
        """取消策略有关的流数据引擎"""
        pass

    @abstractmethod
    def scan(self):
        """ Transaction logic of strategy """
        pass

    def run(self):
        """ Run the strategy """
        if not self._active:
            self.init()
            self._active = True
        self.scan()
        









    










        

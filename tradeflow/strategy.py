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
        









    










        

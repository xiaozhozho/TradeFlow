from abc import abstractmethod

import pandas as pd

from .kernel import Kernel
from .indicator import Indicator


class Strategy:
    """ This strategy class """
    # 子类可声明的框架配置（框架据此自动订阅/建表/建指标引擎）
    # candles = ("candle1m", "candle15m")           # 需要的 K 线周期
    # indicators = {"ma": [5, 15], "rsi": [14]}     # 声明式指标（见 stream/indicators.py）
    candles = ()
    indicators = None

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

    # ***********   行情数据访问（已收盘 K 线 / tick / 指标 / 盘口 / 订单）   ***********

    def _indicator_table(self) -> str:
        """声明 indicators 时读策略专属指标表，否则读框架默认指标表。"""
        if getattr(self, "indicators", None):
            return f"indicator_{self.__class__.__name__}StreamTable"
        return "candleIndicatorStreamTable"

    @staticmethod
    def _channel_where(channel, instId=None) -> str:
        where = f"channel='{channel}'"
        if instId:
            where += f" and instId='{instId}'"
        return where

    def getBars(self, channel="candle1m", limit=100, instId=None):
        """最近 limit 根已收盘 K 线（按品种分组、ts 升序、去重）。"""
        script = (
            f"select top {int(limit)} ts, o, h, l, c, vol, volCcy, volCcyQuote, instId, channel "
            f"from candleConfirmStreamTable where {self._channel_where(channel, instId)} "
            f"context by instId csort ts desc"
        )
        frame = self.kernel.dolphin.runScript(script)
        if frame is None or len(frame) == 0:
            return pd.DataFrame(columns=[
                "ts", "o", "h", "l", "c", "vol", "volCcy", "volCcyQuote", "instId", "channel",
            ])
        return (
            frame.drop_duplicates(subset=["ts", "instId"], keep="last")
            .sort_values(["ts", "instId"])
            .reset_index(drop=True)
        )

    def getTick(self, instId):
        """单品种最新 tick 快照（dict，无数据返回 {}）。"""
        frame = self.kernel.dolphin.runScript(
            f"select * from shareTickTable where instId='{instId}'"
        )
        if frame is None or frame.empty:
            return {}
        return frame.iloc[0].to_dict()

    def getIndicators(self, channel="candle1m", limit=100, instId=None):
        """最近 limit 行指标（按品种分组、ts 升序）。

        未声明 indicators 时读默认指标表 candleIndicatorStreamTable；
        声明后读策略专属指标表（列名为声明生成的 ma_5/rsi_14 等）。
        """
        script = (
            f"select top {int(limit)} * from {self._indicator_table()} "
            f"where {self._channel_where(channel, instId)} "
            f"context by instId csort ts desc"
        )
        frame = self.kernel.dolphin.runScript(script)
        if frame is None or len(frame) == 0:
            return pd.DataFrame()
        return (
            frame.drop_duplicates(subset=["ts", "instId"], keep="last")
            .sort_values(["ts", "instId"])
            .reset_index(drop=True)
        )

    def getBook(self, instId, source="books5", levels=5):
        """最新盘口快照：{"ts", "bids": [(px, sz)...], "asks": [...]}。"""
        frame = self.kernel.dolphin.runScript(
            f"select * from bookStreamTable where instId='{instId}' and source='{source}'"
        )
        empty = {"instId": instId, "source": source, "ts": None, "bids": [], "asks": []}
        if frame is None or frame.empty:
            return empty
        latest_ts = frame["ts"].max()
        snapshot = frame[frame["ts"] == latest_ts]

        def side(side_name):
            rows = snapshot[snapshot["side"] == side_name].sort_values("level").head(levels)
            return [(float(row.px), float(row.sz)) for row in rows.itertuples()]

        return {"instId": instId, "source": source, "ts": latest_ts,
                "bids": side("bid"), "asks": side("ask")}

    def getOrders(self, instId=None, state=None):
        """订单快照列表（shareOrderTable），可按品种/状态过滤。"""
        frame = self.kernel.dolphin.runScript("select * from shareOrderTable")
        if frame is None or frame.empty:
            return []
        records = frame.to_dict("records")
        if instId:
            records = [row for row in records if row.get("instId") == instId]
        if state:
            records = [row for row in records if row.get("state") == state]
        return records

    # ***********   交易便捷方法（薄封装：kernel.trader -> okx SDK）   ***********

    def buy(self, instId, sz, px=None, **kwargs):
        """买入（默认市价）；sz/px 自动按合约规格规整，返回 OKX 原始应答。"""
        return self.kernel.trader.buy(instId, sz, px=px, **kwargs)

    def sell(self, instId, sz, px=None, **kwargs):
        """卖出（默认市价）；reduceOnly=True 表示只减仓。"""
        return self.kernel.trader.sell(instId, sz, px=px, **kwargs)

    def buyByMargin(self, instId, margin_usdt, px=None, **kwargs):
        """按保证金买入：张数 = 保证金 x 杠杆 / (价格 x 合约面值)，含下单前限额校验。"""
        size = self.kernel.trader.sizeByMargin(instId, margin_usdt, price=px)
        return self.buy(instId, size, px=px, **kwargs)

    def sellByMargin(self, instId, margin_usdt, px=None, **kwargs):
        """按保证金卖出（做空/减仓），规则同 buyByMargin。"""
        size = self.kernel.trader.sizeByMargin(instId, margin_usdt, price=px)
        return self.sell(instId, size, px=px, **kwargs)

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
        









    










        

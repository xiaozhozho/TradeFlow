import time
import asyncio
from threading import Thread

from .okx.api import Api
from .risk import RiskManage
from .utils.log import get_logger
from .utils.email import Email
from .stream.dolphin import Dolphin
from .subscribe import PublicSubscribe,CandleSubscribe,PrivateSubscribe


class Kernel:
    """ 
    TradeFlow的核心组件,负责管理所有模块及其组件, 包括: 行情订阅、日志输出、风险管理
    和数据库操作等,它还管理所有的策略的运行,统一交易调度等。
    """
    def __init__(self, pause:int=1, instIds=set(), flag='0', warmup:int=320, private:bool=True):
        """ kernel对象, 框架核心部分, 高度适配和集成功能 """
        self.api = Api(flag=flag)        # api: 使用okx-rest-api进行低频操作
        self.email = Email()             # email: 定时发送交易、账户信息
        self.dolphin = Dolphin()         # dolphin:操作数据库及内存表等
        self.risk = RiskManage(self)     # risk: 风险管理模块
        self.pause = pause               # float: 定期运行停顿时间
        self.warmup = warmup             # int: 启动后等待行情/订阅就绪的秒数
        self.private_enabled = private   # bool: 是否订阅私有频道（账户/持仓/订单）
        self.instIds = instIds           # set:  交易币种的集合
        self.flag = flag                 # flag: 是否模拟
        self.logger = get_logger()       # logger: 日志记录
        self.strategies = []             # list: 策略列表
        self._active = False             # bool: kernel工作状态
        self._loop = asyncio.new_event_loop()  # 订阅的事件循环
        self._tasks = []                        # 订阅协程句柄, 用于退出时取消
        self._thread = Thread(target=self._start, daemon=True)        # 事件循环子线程,用于行情异步订阅以及风险管理

    def _start(self):
        """ 启动当前事件循环 """
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def addStrategy(self, strategy_class):
        """ 
        添加策略, 同时将策略参数更新到kernel 
        params:
            strategy_class: 策略类
        """
        strategy = strategy_class(self)
        self.strategies.append(strategy)
        self.instIds = self.instIds.union(strategy.instIds)
        
    @property
    def publicChannels(self):
        """ 公共频道订阅 channels """
        channels = []
        tick =  [{"channel": "tickers", "instId": instId} for instId in self.instIds]
        channels.extend(tick)
        return channels
    
    @property
    def candleChannels(self):
        """ K线行情频道订阅 channels """
        channels = []
        candle1m = [{"channel": "candle1m", "instId": instId} for instId in self.instIds]
        candle5m = [{"channel": "candle5m", "instId": instId} for instId in self.instIds]
        channels.extend(candle1m)
        channels.extend(candle5m)
        return channels

    @property
    def privateChannels(self):
        """ 私有频道订阅 channels """
        channels = []
        account = [{"channel": "account"}]
        order = [{"channel": "orders", "instType": "ANY"}]
        position = [{"channel": "positions", "instType": "ANY"}]
        warning = [{"channel": "liquidation-warning","instType": "ANY"}]
        channels.extend(account)
        channels.extend(position)
        channels.extend(order)
        channels.extend(warning)
        return channels

    def subscribe(self):
        """ 启动websocket订阅线程, 将数据写入流数据表,同时开启风险管理子线程 """
        self._thread.start()
        self.public = PublicSubscribe(self, self.publicChannels)
        self.candle = CandleSubscribe(self, self.candleChannels)
        self._tasks = [
            asyncio.run_coroutine_threadsafe(self.public.run(), self._loop),
            asyncio.run_coroutine_threadsafe(self.candle.run(), self._loop),
        ]
        if self.private_enabled:
            self.private = PrivateSubscribe(self, self.privateChannels)
            self._tasks.append(asyncio.run_coroutine_threadsafe(self.private.run(), self._loop))

    def prepare(self):
        """dolphindb中运行预处理脚本""" 
        self.dolphin.unsubscribeTable('tickStreamTable', 'lastTickAction')
        self.dolphin.createShareTickTable()
        self.dolphin.createShareWarningTable()
        self.dolphin.createShareAccountTable()
        self.dolphin.createSharePositionTable()
        self.dolphin.createTickStreamTable()
        self.dolphin.createCandleStreamTable()
        self.dolphin.createOrderStreamTable()
        self.dolphin.createLastTickEngine()

    def init(self):
        """ 初始化kernel, 对所有strategy调用初始化前运行函数preinit"""
        for strategy in self.strategies:
            strategy.preinit()

        self.prepare()
        self.subscribe()
        if self.private_enabled:
            try:
                self.api.account.set_position_mode('net_mode')
            except Exception as e:
                # 未配置密钥或无交易权限时忽略，公共行情仍可正常订阅
                self.logger.warning(f'<Kernel>: 设置持仓模式失败，按公共行情模式继续: {e}')
        self._active = True
        if self.warmup > 0:
            time.sleep(self.warmup)

    def getTradeInstid(self,limit=20):
        """筛选流动性较好的instid作为交易对象"""
        script = f"exec instId from shareTickIndicatorTable order by count5m desc limit {limit}"
        instids = self.dolphin.runScript(script)
        return instids

    def getLastAccount(self):
        """ 获取账户资金信息 """
        account = self.dolphin.runScript('shareAccountTable').set_index('ccy').T.to_dict()
        return account
    
    def getLastPositions(self):
        """ 获取持仓情况 """
        positions = self.dolphin.runScript('sharePositionTable').set_index('instId').T.to_dict()
        return positions

    def log(self, msg):
        """ 输出日志 """
        self.logger.info(f"<Kernel>: {msg}")

    def run(self):
        """ 采用循环运行的模式, 巡查交易条件的触发情况 """
        self.risk.run()
        for strategy in self.strategies:
            strategy.run()
            
    def start(self):
        """ 启动kernel,遍历运行策略 """
        if not self._active:
            self.init()
        while self._active:
            self.run()
            time.sleep(self.pause)

 
    def stop(self):
        """ 停止kernel及其行情订阅线程 """
        self._active = False
        for name in ('public', 'candle', 'private'):
            subscribe = getattr(self, name, None)
            if subscribe is not None:
                subscribe.active = False
        for task in self._tasks:
            task.cancel()
        if self._loop.is_running():
            time.sleep(0.5)  # 留出时间让 websocket 连接优雅关闭
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread.is_alive():
            self._thread.join(timeout=5)

        
        

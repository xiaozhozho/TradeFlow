import time
import asyncio
from threading import Thread

from .okx.api import Api
from .risk import RiskManage
from .utils.log import get_logger
from .utils.email import Email
from .stream.dolphin import Dolphin
from .subscribe import PublicSubscribe,CandleSubscribe,PrivateSubscribe,SubscribeManager
from .subscribe.channels import (
    DEFAULT_ALGO_CHANNELS,
    DEFAULT_CANDLE_CHANNELS,
    DEFAULT_MARKET_CHANNELS,
    DEFAULT_PRIVATE_CHANNELS,
    OPTIONS_CHANNELS,
    get_channel_spec,
    tables_for,
    to_index_id,
    to_inst_family,
)

# 无论订阅哪些频道都必须存在的表（快照表 + 实时总线骨架）
CORE_TABLES = {
    "tickStreamTable", "shareTickTable",
    "candleStreamTable", "candleConfirmStreamTable", "shareCandleTable",
    "orderStreamTable", "shareOrderTable",
    "shareAccountTable", "sharePositionTable", "shareWarningTable",
}


class Kernel:
    """ 
    TradeFlow的核心组件,负责管理所有模块及其组件, 包括: 行情订阅、日志输出、风险管理
    和数据库操作等,它还管理所有的策略的运行,统一交易调度等。
    """
    def __init__(self, pause:int=1, instIds=set(), flag='0', warmup:int=320, private:bool=True,
                 market=None, depth=None, options=False, algo=False, recreate_tables=False):
        """ kernel对象, 框架核心部分, 高度适配和集成功能
        params:
            pause: 策略轮询间隔（秒）
            instIds: 交易币种集合
            flag: '0' 实盘 / '1' 模拟盘
            warmup: 启动后等待行情/订阅就绪的秒数
            private: 是否订阅私有频道（账户/持仓/订单）
            market: 公共行情频道集合，None 用 DEFAULT_MARKET_CHANNELS
            depth: 盘口深度频道，None/"books5"/"bbo-tbt"/"books"
            options: 是否订阅期权频道（opt-summary/option-trades/estimated-price）
            algo: 是否订阅策略委托频道（orders-algo/algo-advance）
            recreate_tables: prepare() 时是否删除重建流表（开发环境用，会丢数据）
        """
        self.api = Api(flag=flag)        # api: 使用okx-rest-api进行低频操作
        self.email = Email()             # email: 定时发送交易、账户信息
        self.dolphin = Dolphin()         # dolphin:操作数据库及内存表等
        self.risk = RiskManage(self)     # risk: 风险管理模块
        self.pause = pause               # float: 定期运行停顿时间
        self.warmup = warmup             # int: 启动后等待行情/订阅就绪的秒数
        self.private_enabled = private   # bool: 是否订阅私有频道（账户/持仓/订单）
        self.instIds = instIds           # set:  交易币种的集合
        self.flag = flag                 # flag: 是否模拟
        self.market = market             # tuple/None: 公共行情频道
        self.depth = depth               # str/None: 盘口频道
        self.options_enabled = options   # bool: 期权频道
        self.algo_enabled = algo         # bool: 策略委托频道
        self.recreate_tables = recreate_tables  # bool: 重建流表
        self.reconciler = None           # Reconciler: 断线对账（后续任务接入）
        self.manager = None              # SubscribeManager: 订阅编排
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

    # ***********   频道编排（频道清单唯一来源在 subscribe/channels.py）   **************

    def enabled_channels(self) -> set:
        """当前配置下会订阅的全部频道名。"""
        names = set(self.market or DEFAULT_MARKET_CHANNELS)
        names.update(DEFAULT_CANDLE_CHANNELS)
        if self.depth:
            names.add(self.depth)
        if self.options_enabled:
            names.update(OPTIONS_CHANNELS)
        if self.private_enabled:
            names.update(DEFAULT_PRIVATE_CHANNELS)
        if self.algo_enabled:
            names.update(DEFAULT_ALGO_CHANNELS)
        return names

    @property
    def publicChannels(self):
        """ 公共频道订阅 channels（含 depth/options 开关） """
        channels = []
        for name in self.market or DEFAULT_MARKET_CHANNELS:
            if name == "status":
                channels.append({"channel": name})
            elif name in ("instruments", "liquidation-orders", "estimated-price"):
                channels.append({"channel": name, "instType": "SWAP"})
            elif name == "index-tickers":
                channels.extend(
                    {"channel": name, "instId": to_index_id(instId)}
                    for instId in self.instIds
                )
            else:
                channels.extend(
                    {"channel": name, "instId": instId}
                    for instId in self.instIds
                )
        if self.depth:
            channels.extend(
                {"channel": self.depth, "instId": instId}
                for instId in self.instIds
            )
        if self.options_enabled:
            families = {
                family for family in (to_inst_family(instId) for instId in self.instIds)
                if family is not None
            }
            channels.extend(
                {"channel": "opt-summary", "instFamily": family}
                for family in families
            )
            channels.extend(
                {"channel": "option-trades", "instFamily": family}
                for family in families
            )
        return channels

    @property
    def candleChannels(self):
        """ K线行情频道订阅 channels（业务端点） """
        return [
            {"channel": channel, "instId": instId}
            for instId in self.instIds
            for channel in DEFAULT_CANDLE_CHANNELS
        ]

    @property
    def privateChannels(self):
        """ 私有频道订阅 channels """
        channels = []
        for name in DEFAULT_PRIVATE_CHANNELS:
            if name in ("account", "balance_and_position", "account-greeks"):
                channels.append({"channel": name})
            else:
                channels.append({"channel": name, "instType": "ANY"})
        return channels

    @property
    def algoChannels(self):
        """ 策略委托频道订阅 channels（业务端点，需登录） """
        return [
            {"channel": name, "instType": "ANY"}
            for name in DEFAULT_ALGO_CHANNELS
        ]

    # ***********   启停与建表   **************

    def subscribe(self):
        """ 启动websocket订阅线程, 将数据写入流数据表,同时开启风险管理子线程 """
        if not self._thread.is_alive():
            self._thread.start()
        self.manager = SubscribeManager(self)
        self.manager.start()
        # 兼容旧用法：kernel.public / candle / private / algo 直接可访问
        self.public = self.manager.public
        self.candle = self.manager.business
        self.private = self.manager.private
        self.algo = self.manager.algo

    def prepare(self):
        """dolphindb中运行预处理脚本：按频道建表 + 建流引擎"""
        if self.recreate_tables:
            self._cleanup_engines_before_recreate()
        tables = set(CORE_TABLES) | tables_for(self.enabled_channels())
        self.dolphin.prepare_streams(tables, recreate=self.recreate_tables)
        from .stream.engines import ENGINE_NAMES
        self.dolphin.prepare_engines(ENGINE_NAMES)

    def _cleanup_engines_before_recreate(self):
        """重建流表前先摘除引擎订阅，否则活跃订阅会阻塞 dropStreamTable。"""
        from .stream.engines import ENGINE_SPECS
        for spec in ENGINE_SPECS.values():
            self.dolphin.unsubscribeTable(spec.source, spec.action)
            self.dolphin.dropStreamEngine(spec.name)

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
        """ 获取持仓快照
        net 模式返回 {instId: 字段字典}；双向持仓返回 {"instId:posSide": 字段字典}。
        """
        df = self.dolphin.runScript('select * from sharePositionTable')
        if df is None or df.empty:
            return {}
        long_short = 'posSide' in df.columns and (df['posSide'].fillna('') != '').any()
        if long_short:
            return {
                f"{row.instId}:{row.posSide}": row._asdict()
                for row in df.itertuples(index=False)
            }
        return df.set_index('instId').T.to_dict()

    def getPositions(self):
        """ 持仓列表（供 RiskManage 使用，双向持仓每行独立） """
        df = self.dolphin.runScript('select * from sharePositionTable')
        if df is None or df.empty:
            return []
        return df.to_dict('records')

    def getLastOrders(self):
        """ 最新订单快照 """
        return self.dolphin.runScript('select * from shareOrderTable')

    def getLastCandle(self, channel='candle1m'):
        """ 最新 K 线快照（含未收盘 bar） """
        return self.dolphin.runScript(
            f"select * from shareCandleTable where channel='{channel}'"
        )

    def health(self):
        """ 订阅健康指标（连接状态/重连计数/写队列等），由 SubscribeManager 提供 """
        return self.manager.health() if self.manager is not None else {}

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
        if self.manager is not None:
            self.manager.stop()
        for task in self._tasks:
            task.cancel()
        if self._loop.is_running():
            time.sleep(0.5)  # 留出时间让 websocket 连接优雅关闭
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread.is_alive():
            self._thread.join(timeout=5)

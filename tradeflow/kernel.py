import time
import asyncio
from threading import Thread

from .okx.api import Api
from .risk import RiskManage
from .trade import Trader
from .utils.log import get_logger
from .utils.email import Email
from .stream.dolphin import Dolphin
from .stream.engines import ENGINE_SPECS
from .subscribe import PublicSubscribe,CandleSubscribe,PrivateSubscribe,SubscribeManager
from .subscribe.reconcile import Reconciler
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


def prepare_table_set(channels) -> set:
    """建表集合 = 核心表 ∪ 启用频道表 ∪ 全部流引擎的输入/输出表。

    引擎表（如指标表）不隶属任何频道，且部分引擎（策略委托）无条件创建；
    若不预先建表，新库首次 prepare() 会在建引擎时因表不存在而失败。
    """
    tables = set(CORE_TABLES) | tables_for(channels)
    for spec in ENGINE_SPECS.values():
        tables.add(spec.source)
        tables.add(spec.target)
    return tables


class Kernel:
    """ 
    TradeFlow的核心组件,负责管理所有模块及其组件, 包括: 行情订阅、日志输出、风险管理
    和数据库操作等,它还管理所有的策略的运行,统一交易调度等。
    """
    def __init__(self, pause:int=1, instIds=set(), flag='1', warmup:int=320, private:bool=True,
                 market=None, depth=None, options=False, algo=False, recreate_tables=False,
                 max_errors:int=10, dolphin=None, api=None,
                 candles=None, archive=False, max_order_margin=None, max_position_margin=None):
        """ kernel对象, 框架核心部分, 高度适配和集成功能
        params:
            pause: 策略轮询间隔（秒）
            instIds: 交易币种集合
            flag: '0' 实盘 / '1' 模拟盘（默认模拟盘，实盘启动会打印警告）
            warmup: 启动后等待行情/订阅就绪的秒数
            private: 是否订阅私有频道（账户/持仓/订单）
            market: 公共行情频道集合，None 用 DEFAULT_MARKET_CHANNELS
            depth: 盘口深度频道，None/"books5"/"bbo-tbt"/"books"
            options: 是否订阅期权频道（opt-summary/option-trades/estimated-price）
            algo: 是否订阅策略委托频道（orders-algo/algo-advance）
            recreate_tables: prepare() 时是否删除重建流表（开发环境用，会丢数据）
            max_errors: 策略连续异常多少次后自动停用（默认 10）
            dolphin: 可选注入的 DolphinDB 客户端（离线测试用 FakeDolphin）
            api: 可选注入的 OKX API 客户端（离线测试用 FakeApi）
            candles: K 线周期元组，默认 ("candle1m", "candle5m")；
                     策略类属性 candles 会并入（多周期）
            archive: 是否将 tick/trade 归档到 dfs://market（回测数据）
            max_order_margin: 单笔保证金上限（USDT），None 不限
            max_position_margin: 单品种持仓保证金上限（USDT），None 不限
        """
        self.api = api if api is not None else Api(flag=flag)  # api: 使用okx-rest-api进行低频操作
        self.email = Email()             # email: 定时发送交易、账户信息
        self.dolphin = dolphin if dolphin is not None else Dolphin()  # dolphin:操作数据库及内存表等
        self.risk = RiskManage(self)     # risk: 风险管理模块
        self.trader = Trader(self)       # trader: 交易便捷层（规整数量/价格 + 下单/平仓）
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
        self.candles = tuple(candles) if candles else DEFAULT_CANDLE_CHANNELS  # tuple: K 线周期
        self.archive_enabled = archive     # bool: tick/trade 归档到 dfs://market
        self.max_order_margin = max_order_margin          # float|None: 单笔保证金上限（USDT）
        self.max_position_margin = max_position_margin    # float|None: 单品种持仓保证金上限（USDT）
        self.reconciler = Reconciler(self)   # Reconciler: 断线对账（REST 快照覆盖 + K 线补齐）
        self.manager = None              # SubscribeManager: 订阅编排
        self.logger = get_logger()       # logger: 日志记录
        if str(self.flag) == '0':
            self.logger.warning(
                "<Kernel>: 实盘模式（flag='0'）已启用：下单将使用真实资金，请确认密钥与策略逻辑"
            )
        self.strategies = []             # list: 策略列表
        self.max_errors = max_errors     # int: 策略连续异常停用阈值
        self._risk_errors = 0            # int: 风控累计异常次数
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
        declared = tuple(getattr(strategy, "candles", ()) or ())
        if declared:
            self.candles = tuple(dict.fromkeys(self.candles + declared))

    # ***********   频道编排（频道清单唯一来源在 subscribe/channels.py）   **************

    def enabled_channels(self) -> set:
        """当前配置下会订阅的全部频道名。"""
        names = set(self.market or DEFAULT_MARKET_CHANNELS)
        names.update(self.candles)
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
        """ K线行情频道订阅 channels（业务端点，周期来自 candles 参数/策略声明） """
        return [
            {"channel": channel, "instId": instId}
            for instId in self.instIds
            for channel in self.candles
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
        """dolphindb中运行预处理脚本：按频道建表 + 声明式指标 + 建流引擎"""
        if self.recreate_tables:
            self._cleanup_engines_before_recreate()
        from .stream.indicators import register_strategy_indicators
        tables = prepare_table_set(self.enabled_channels())
        custom_tables, _ = register_strategy_indicators(self.strategies)
        tables |= custom_tables
        self.dolphin.prepare_streams(tables, recreate=self.recreate_tables)
        self.dolphin.prepare_engines(tuple(ENGINE_SPECS))
        if self.archive_enabled:
            from .stream.archive import ensure_archive
            ensure_archive(self.dolphin)

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
        """ 订阅健康指标（连接/重连/写队列）+ 风控/策略统计 + 数据时效 """
        stats = self.manager.health() if self.manager is not None else {}
        stats["risk"] = {"errors": self._risk_errors}
        stats["strategies"] = {
            f"{strategy.__class__.__name__}#{index}": {
                "errors": getattr(strategy, "_error_count", 0),
                "disabled": bool(getattr(strategy, "disabled", False)),
            }
            for index, strategy in enumerate(self.strategies)
        }
        stats["freshness"] = self._data_freshness()
        return stats

    def _data_freshness(self) -> dict:
        """tick/1m K 线最新数据距今秒数（2 秒缓存；查询失败为 None）。"""
        now_monotonic = time.monotonic()
        if getattr(self, "_freshness", None) and now_monotonic - getattr(self, "_freshness_at", 0) < 2:
            return dict(self._freshness)
        import pandas as pd

        now = pd.Timestamp.now(tz="UTC").tz_localize(None)
        freshness = {}
        for key, table in (("tick", "tickStreamTable"), ("candle1m", "candleConfirmStreamTable")):
            try:
                latest = self.dolphin.runScript(f"exec max(ts) from {table}")
                if latest is None or pd.isna(latest):
                    freshness[key] = None
                else:
                    freshness[key] = round((now - pd.Timestamp(latest)).total_seconds(), 1)
            except Exception:
                freshness[key] = None
        self._freshness_at = now_monotonic
        self._freshness = freshness
        return dict(freshness)

    def log(self, msg):
        """ 输出日志 """
        self.logger.info(f"<Kernel>: {msg}")

    def run(self):
        """ 采用循环运行的模式, 巡查交易条件的触发情况

        单个策略/风控异常只记录并跳过本轮，不终止主循环；
        策略连续异常达到 max_errors 次后自动停用（disabled），成功一次即清零。
        """
        try:
            self.risk.run()
        except Exception:
            self._risk_errors += 1
            self.logger.exception("<Kernel>: 风控执行异常，已跳过本轮")

        for strategy in self.strategies:
            if getattr(strategy, "disabled", False):
                continue
            try:
                strategy.run()
            except Exception:
                strategy._error_count = getattr(strategy, "_error_count", 0) + 1
                self.logger.exception(
                    f"<Kernel>: 策略 {strategy.__class__.__name__} 执行异常"
                    f"（连续 {strategy._error_count} 次）"
                )
                if strategy._error_count >= self.max_errors:
                    strategy.disabled = True
                    self.logger.error(
                        f"<Kernel>: 策略 {strategy.__class__.__name__} 连续异常达到 "
                        f"{self.max_errors} 次，已自动停用"
                    )
            else:
                strategy._error_count = 0
            
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

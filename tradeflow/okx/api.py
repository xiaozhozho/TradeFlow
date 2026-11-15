"""OKX API 聚合入口（项目适配层，非上游文件）。

上游 python-okx 只提供按业务拆分的 API 类（AccountAPI、TradeAPI 等），
本项目需要 `Api` 这一统一入口，因此在此做一层轻量聚合：

    api = Api(flag='0')
    api.account.get_account_balance()
    api.trade.place_order(...)
    api.market.get_ticker('BTC-USDT')

维护提示：升级 okx 目录时保留本文件，并核对上游新增/改名的 API 类。
"""
from ..setting import (
    OKX_API_KEY,
    OKX_SECRETE_KEY,
    OKX_PASSPHRASE,
    OKX_API_KEY_SIMULATE,
    OKX_SECRETE_KEY_SIMULATE,
    OKX_PASSPHRASE_SIMULATE,
)
from .Account import AccountAPI
from .Affiliate import AffiliateAPI
from .BlockTrading import BlockTradingAPI
from .Convert import ConvertAPI
from .CopyTrading import CopyTradingAPI
from .DualInvest import DualInvestAPI
from .FDBroker import FDBrokerAPI
from .Funding import FundingAPI
from .Grid import GridAPI
from .MarketData import MarketAPI
from .PublicData import PublicAPI
from .SpreadTrading import SpreadTradingAPI
from .Status import StatusAPI
from .SubAccount import SubAccountAPI
from .Trade import TradeAPI
from .TradingData import TradingDataAPI


class Api:
    """将所有 api 功能统一封装在一起。

    params:
        flag: '0' 实盘 / '1' 模拟盘（与 OKX 的 x-simulated-trading 头一致）
        debug: 是否打印签名、请求头等调试信息
        proxy: 可选 HTTP(S) 代理，透传给底层 httpx.Client

    note:
        上游已废弃 `use_server_time`，保留该参数仅为兼容旧调用；传入不会生效。
    """

    def __init__(self, use_server_time=False, flag='0', debug=False, proxy=None) -> None:
        self.flag = '0' if str(flag) == '0' else '1'
        self.debug = debug

        # 属性：按实盘/模拟盘选择密钥；空字符串视为未配置，降级为公共接口
        if self.flag == '0':
            self.api_key = OKX_API_KEY or '-1'
            self.secrete_key = OKX_SECRETE_KEY or '-1'
            self.passphrase = OKX_PASSPHRASE or '-1'
        else:
            self.api_key = OKX_API_KEY_SIMULATE or '-1'
            self.secrete_key = OKX_SECRETE_KEY_SIMULATE or '-1'
            self.passphrase = OKX_PASSPHRASE_SIMULATE or '-1'

        # 函数及功能
        kwargs = dict(
            api_key=self.api_key,
            api_secret_key=self.secrete_key,
            passphrase=self.passphrase,
            flag=self.flag,
            debug=self.debug,
            proxy=proxy,
        )
        self.account = AccountAPI(**kwargs)
        self.affiliate = AffiliateAPI(**kwargs)
        self.blocktrading = BlockTradingAPI(**kwargs)
        self.convert = ConvertAPI(**kwargs)
        self.copytrading = CopyTradingAPI(**kwargs)
        self.dualinvest = DualInvestAPI(**kwargs)
        self.fdbroker = FDBrokerAPI(**kwargs)
        self.funding = FundingAPI(**kwargs)
        self.grid = GridAPI(**kwargs)
        self.market = MarketAPI(**kwargs)
        self.public = PublicAPI(**kwargs)
        self.spreadtrading = SpreadTradingAPI(**kwargs)
        self.status = StatusAPI(**kwargs)
        self.subaccount = SubAccountAPI(**kwargs)
        self.trade = TradeAPI(**kwargs)
        self.tradingdata = TradingDataAPI(**kwargs)

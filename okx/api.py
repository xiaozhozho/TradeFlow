from ..setting import OKX_API_KEY_SIMULATE,OKX_SECRETE_KEY_SIMULATE,OKX_PASSPHRASE_SIMULATE,OKX_API_KEY,OKX_SECRETE_KEY,OKX_PASSPHRASE
from .Account import AccountAPI
from .BlockTrading import BlockTradingAPI
from .Convert import ConvertAPI
from .Earning import EarningAPI
from .Funding import FundingAPI
from .Grid import GridAPI
from .MarketData import MarketAPI
from .NDBroker import NDBrokerAPI
from .PublicData import PublicAPI
from .Status import StatusAPI
from .SubAccount import SubAccountAPI
from .Trade import TradeAPI
from .TradingData import TradingDataAPI



class Api:
    """ 将所有api功能统一封装在一起 """
    def __init__(self, use_server_time=False, flag='0', debug=False) -> None:
        # 属性
        self.api_key = OKX_API_KEY if flag=='0' else OKX_API_KEY_SIMULATE
        self.secrete_key = OKX_SECRETE_KEY if flag=='0' else OKX_SECRETE_KEY_SIMULATE
        self.passphrase = OKX_PASSPHRASE if flag=='0' else OKX_PASSPHRASE_SIMULATE
        self.use_server_time = use_server_time
        self.flag = flag # 模拟 1, 实盘 0.
        self.debug = debug

        # 函数及功能
        self.account = AccountAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.blocktrading = BlockTradingAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.convert = ConvertAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.earning = EarningAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.funding = FundingAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.grid = GridAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.market = MarketAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.ndbroker = NDBrokerAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.public = PublicAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.status = StatusAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.subaccount = SubAccountAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.trade = TradeAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)
        self.tradingdata = TradingDataAPI(self.api_key, self.secrete_key, self.passphrase, self.use_server_time, self.flag, debug=self.debug)



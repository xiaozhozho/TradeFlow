import time
import asyncio
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta

from ..okx.PublicData import PublicAPI
from ..okx.MarketData import MarketAPI
from .log import get_logger
from ..stream.dolphin import Dolphin
from ..setting import OKX_API_KEY, OKX_SECRETE_KEY,OKX_PASSPHRASE

logger = get_logger()

public = PublicAPI(OKX_API_KEY or '-1', OKX_SECRETE_KEY or '-1', OKX_PASSPHRASE or '-1', flag='0', debug=False)
market = MarketAPI(OKX_API_KEY or '-1', OKX_SECRETE_KEY or '-1', OKX_PASSPHRASE or '-1', flag='0', debug=False)


def datetime_to_ts(dt):
    """将datetime格式日期转换成ts"""
    timestamp = datetime.timestamp(dt) * 1000 
    return int(timestamp)

def dfApply(df: pd.DataFrame, func):
    """DataFrame 逐元素映射，兼容 pandas 1.x(applymap) 与 2.1+/3.x(map)。"""
    if hasattr(df, "map"):
        return df.map(func)
    return df.applymap(func)

# 时间转换统一走 utils.format（UTC-naive），此处仅保留兼容重导出。
from .format import ts2Timestamp  # noqa: E402  pylint: disable=ungrouped-imports

def get_instruments():
    """获取所有永续合约信息"""
    data = public.get_instruments('SWAP')['data']
    instruments = pd.DataFrame(data)
    return instruments

def get_all_usdt_swap():
    """ 获取USDT 所有合约列表  """
    instrument = get_instruments()
    instrument['is_USDT'] = instrument.instFamily.str.split('-').apply(lambda x:x[1])
    swaps = instrument.query("`is_USDT`=='USDT'").instId.to_list()
    return swaps

def get_swap_candle_start_dict():
    """获取数据库中每个合约的行情数据的起点,方便向前抓取数据"""
    script = """
    pt = loadTable('dfs://swapCandleLib', 'swapCandleTable')
    start = select instId, min(ts) as ts from pt group by instId
    swap_start = dict(exec instId from start, exec datetime(ts) as datetime from start)
    swap_start
    """
    swap_start_dict = Dolphin().runScript(script)
    return swap_start_dict

def supply_swap_data():
    """ 根据数据库最旧的数据向前继续下载数据 """
    dbPath = 'dfs://swapCandleLib'
    tableName ='swapCandleTable'
    swap_start_dict = get_swap_candle_start_dict()
    instIds = list(swap_start_dict.keys())
    delta1min = timedelta(minutes=1)
    for instId in tqdm(instIds, ncols=60, colour='green'):
        try:
            data_start = swap_start_dict[instId]
            end = data_start.item() - delta1min
            df = get_minute_candle(instId, end)
            if df is not None and len(df)>0:
                Dolphin().df2Table(dbPath, tableName, df)
                print(f'Data success for {instId}, number:{len(df)}')
            else:
                logger.info(f'No data for {instId}')
        except Exception as e:
            logger.info(f'ERROR:{e}')

def get_minute_candle(instId, end, sleep=0.12):
    """给定终止日期,自动向前获取分钟Candle数据"""
    delta = timedelta(minutes=100)
    data_list = []
    while True:
        try:
            start = end - delta
            data = market.get_history_candlesticks(instId, datetime_to_ts(end)+60000, datetime_to_ts(start), '1m')['data']
            if data:
                data_list.extend(data)
                end = start
                time.sleep(sleep)
            else:
                break
 
        except Exception as e:
            logger.info(f'{instId} | {end} | {e}')
            break
    if data_list:
        df = pd.DataFrame(data_list)
        df = df.astype('float64')
        df.columns = ['ts', 'o', 'h', 'l', 'c', 'vol',  'volCcy', 'volCcyQuote', 'confirm']
        df['confirm'] = df['confirm'].astype(int)
        df.insert(9,'instId',[instId]*len(df))
        df.insert(10,'channel',['channel']*len(df))
        df.sort_values('ts', inplace=True)
        df['ts'] = df['ts'].apply(ts2Timestamp)
    else:
        logger.info(f'{instId} data is empty!')
        df = None
    return df

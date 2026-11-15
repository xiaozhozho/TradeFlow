"""DolphinDB 建表脚本。

列定义的唯一来源是 ``schema.py``（TableSpec 注册表），本文件只负责
保持旧常量名兼容（``dolphin.py`` 等模块仍按名引用）。
脚本均为幂等写法：表已存在则跳过，不会删除已有数据。
"""
from .schema import (
    ACCOUNT, ACCOUNT_GREEKS, ALGO_ORDER, BALANCE, BOOK, CANDLE,
    CANDLE_CONFIRM, CANDLE_INDICATOR, ESTIMATED_PRICE, FILLS, FUNDING,
    INDEX_CANDLE, INDEX_TICK, LIQUIDATION_ORDER, MARK_PRICE,
    MARK_PRICE_CANDLE, OPEN_INTEREST, OPT_SUMMARY, ORDER, OPTION_TRADE,
    POSITION, PRICE_LIMIT, PRODUCT, SHARE_ACCOUNT_GREEKS,
    SHARE_ALGO_ORDER, SHARE_CANDLE, SHARE_CANDLE_INDICATOR, SHARE_ORDER,
    SHARE_TICK, SHARE_TICK_INDICATOR, STATUS, TICK, TICK_INDICATOR,
    TRADE, WARNING,
)

# ************ 1.公共频道-流数据表 **********
createTickStreamTableScript = TICK.ensure_script()
createShareTickTableScript = SHARE_TICK.ensure_script()
createCandleStreamTableScript = CANDLE.ensure_script()
createCandleConfirmStreamTableScript = CANDLE_CONFIRM.ensure_script()
createOpenInterestStreamTableScript = OPEN_INTEREST.ensure_script()
createEstimatedPriceStreamTableScript = ESTIMATED_PRICE.ensure_script()
createMarkPriceStreamTableScript = MARK_PRICE.ensure_script()
createMarkPriceCandleStreamTableScript = MARK_PRICE_CANDLE.ensure_script()
createPriceLimitStreamTableScript = PRICE_LIMIT.ensure_script()
createOptSummaryStreamTableScript = OPT_SUMMARY.ensure_script()
createFundingRateStreamTableScript = FUNDING.ensure_script()
createIndexCandleStreamTableScript = INDEX_CANDLE.ensure_script()
createIndexTickStreamTableScript = INDEX_TICK.ensure_script()
createOptionTradeStreamTableScript = OPTION_TRADE.ensure_script()
createLiquidationOrderStreamTableScript = LIQUIDATION_ORDER.ensure_script()
createProductStreamTableScript = PRODUCT.ensure_script()
createStatusStreamTableScript = STATUS.ensure_script()

# ************ 盘口 / 逐笔 / 指标 **********
createTradeStreamTableScript = TRADE.ensure_script()
createBookStreamTableScript = BOOK.ensure_script()
createCandleIndicatorStreamTableScript = CANDLE_INDICATOR.ensure_script()
createShareCandleIndicatorTableScript = SHARE_CANDLE_INDICATOR.ensure_script()
createTickIndicatorStreamTableScript = TICK_INDICATOR.ensure_script()
createShareTickIndicatorTableScript = SHARE_TICK_INDICATOR.ensure_script()

# ************ 2.私有频道-流数据表 ************
createShareAccountTableScript = ACCOUNT.ensure_script()
createSharePositionTableScript = POSITION.ensure_script()
createShareBalanceTableScript = BALANCE.ensure_script()
createOrderStreamTableScript = ORDER.ensure_script()
createShareOrderTableScript = SHARE_ORDER.ensure_script()
createAlgoOrderStreamTableScript = ALGO_ORDER.ensure_script()
createShareAlgoOrderTableScript = SHARE_ALGO_ORDER.ensure_script()
createFillsStreamTableScript = FILLS.ensure_script()
createShareWarningTableScript = WARNING.ensure_script()
createAccountGreeksStreamTableScript = ACCOUNT_GREEKS.ensure_script()
createShareAccountGreeksTableScript = SHARE_ACCOUNT_GREEKS.ensure_script()

# ************ 3.创建分区数据库脚本***********
createCandleLibScript = """
clearAllCache()

instIds = ['BTC-USDT-SWAP','CRV-USDT-SWAP','ETH-USDT-SWAP']
intervals = [1,5,15,30,60,180,240,1440]
months = 2010.01M..2025.12M

instidDB = database(, VALUE, instIds)
intervalDB = database(, VALUE, intervals)
monthDB = database(, VALUE, months)

colName = ['datetime', 'instId', 'open', 'high', 'low', 'close', 'volume','volCcy', 'volCcyQuote', 'interval']
colType = [TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG]
t = table(200000:0, colName, colType)

db = database("dfs://candleLib", COMPO, [instidDB, intervalDB, monthDB])
pt = db.createPartitionedTable(t, `candleTable, `instId`interval`datetime)
pt.append!(t)
"""

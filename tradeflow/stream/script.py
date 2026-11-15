# 固定一些交易时用到的流数据表，方便统一创建和管理等

# ************ 1.公共频道-流数据表 **********
# 创建 tick 流数据表
createTickStreamTableScript = """
colName = ['instType','instId','last','lastSz','askPx','askSz','bidPx','bidSz','open24h','high24h','low24h','volCcy24h','vol24h','sodUtc0','sodUtc8','ts']
colType = [SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,TIMESTAMP]
tick = streamTable(200000:0, colName, colType)
enableTableShareAndPersistence(tick, "tickStreamTable", false, true, preCache=0, cacheSize=200000, retentionMinutes=360);
"""

# 创建最新tick数据内存表
createShareTickTableScript = """
colName = ['instType','instId','last','lastSz','askPx','askSz','bidPx','bidSz','open24h','high24h','low24h','volCcy24h','vol24h','sodUtc0','sodUtc8','ts']
colType = [SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,TIMESTAMP]
tickTable = latestIndexedTable(`instId, `ts, 1000:0, colName, colType)
share tickTable as shareTickTable
"""

# 创建 candle 流数据表
createCandleStreamTableScript = """
colName = ['ts','o','h','l','c','vol','volCcy','volCcyQuote','confirm','instId','channel']
colType = [TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, SYMBOL, SYMBOL]
candle = streamTable(500000:0, colName, colType)
enableTableShareAndPersistence(candle, "candleStreamTable", false, true, preCache=0, cacheSize=200000, retentionMinutes=180);
"""

# 创建 open-interest 流数据表
createOpenInterestStreamTableScript = """
colName = ['instType','instId','oi','oiCcy','ts']
colType = [SYMBOL, SYMBOL, DOUBLE, DOUBLE, LONG]
openinterest = streamTable(100000:0, colName, colType)
enableTableShareAndPersistence(openinterest, "openinterestStreamTable", false, true, cacheSize=100000, retentionMinutes=60);
"""

# 创建 estimated-price 流数据表
createEstimatedPriceStreamTableScript = """
colName = ['instType','instId','settlePx','ts']
colType = [SYMBOL, SYMBOL, DOUBLE, LONG]
estimatedprice = streamTable(`100000:0, colName, colType)
enableTableShareAndPersistence(estimatedprice, "estimatedpriceStreamTable", false, true, cacheSize=100000, retentionMinutes=1440);
"""

# 创建 mark-price 流数据表
createMarkPriceStreamTableScript = """
colName = ['instType','instId','markPx','ts']
colType = [SYMBOL, SYMBOL, DOUBLE, LONG]
markprice = streamTable(100000:0, colName, colType)
enableTableShareAndPersistence(markprice, "markpriceStreamTable", false, true, cacheSize=100000, retentionMinutes=1440);
"""

# 创建 market-price-candle
createMarkPriceCandleStreamTableScript = """
colName = ['channel','instId','ts','o','h','l','c','confirm']
colType = [SYMBOL,SYMBOL,LONG,DOUBLE,DOUBLE,DOUBLE,DOUBLE,LONG]
markpricecandle = streamTable(200000:0, colName, colType)
enableTableShareAndPersistence(markpricecandle, "markpricecandleStreamTable", false, true, cacheSize=200000, retentionMinutes=1440);
"""

# 创建 limit-price 流数据表
createPriceLimitStreamTableScript = """
colName = ['instId','buyLmt','sellLmt','ts']
colType = [SYMBOL, DOUBLE, DOUBLE, LONG]
pricelimit = streamTable(100000:0, colName, colType)
enableTableShareAndPersistence(pricelimit, "pricelimitStreamTable", false, true, cacheSize=100000, retentionMinutes=1440);
"""

# 创建 opt-summary 流数据表!!!
createOptSummaryStreamTableScript = """
colName = ['instType','instId','uly','delta','gamma','vega','theta','lever','markVol','bidVol','askVol','realVol','deltaBS','gammaBS','thetaBS','vegaBS','ts','fwdPx']
colType = [SYMBOL,SYMBOL,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,LONG,DOUBLE]
optsummary = streamTable(200000:0, colName, colType)
enableTableShareAndPersistence(optsummary, "optsummaryStreamTable", false, true, cacheSize=200000, retentionMinutes=60);
"""

# 创建 funding-rate 流数据表
createFundingRateStreamTableScript = """
colName = ['fundingRate','fundingTime','instId','instType','nextFundingRate','nextFundingTime']
colType = [DOUBLE, LONG, SYMBOL, SYMBOL, DOUBLE, LONG]
fundingrate = streamTable(10000:0, colName, colType)
enableTableShareAndPersistence(fundingrate, "fundingrateStreamTable", false, true, cacheSize=10000, retentionMinutes=60);
"""

# 创建 index-candle 流数据表
createIndexCandleStreamTableScript = """
colName = ['ts','o','h','l','c','confirm','instId', 'channel']
colType = [LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, SYMBOL, SYMBOL]
indexcandle = streamTable(100000:0, colName, colType)
enableTableShareAndPersistence(indexcandle, "indexcandleStreamTable", false, true, cacheSize=100000, retentionMinutes=180);
"""

# 创建 index-tickers 流数据表
createIndexTickStreamTableScript = """
colName = ['instId','idxPx','high24h','low24h','open24h','sodUtc0','sodUtc8','ts']
colType = [SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,LONG]
indextick = streamTable(200000:0, colName, colType)
enableTableShareAndPersistence(indextick, "indextickStreamTable", false, true, cacheSize=200000, retentionMinutes=180);
"""

# 创建 option-trades 流数据表
createOptionTradeStreamTableScript = """
colName = ['fillVol','fwdPx','indexPx','instFamily','instId','markPx','optType','px','side','sz','tradeId','ts']
colType = [DOUBLE,DOUBLE,DOUBLE,SYMBOL,SYMBOL,DOUBLE,SYMBOL,DOUBLE,SYMBOL,DOUBLE,LONG,LONG]
optiontrade = streamTable(50000:0, colName, colType)
enableTableShareAndPersistence(optiontrade, "optiontradeStreamTable", false, true, cacheSize=50000, retentionMinutes=180);
"""

# 创建 liquidation-orders 流数据表
createLiquidationOrderStreamTableScript = """
colName = ['bkLoss','bkPx','ccy','posSide','side','sz','ts','instId','instType','uly']
colType = [DOUBLE,DOUBLE,SYMBOL,SYMBOL,SYMBOL,DOUBLE,LONG,SYMBOL,SYMBOL,SYMBOL]
liquidationorder = streamTable(10000:0, colName, colType)
enableTableShareAndPersistence(liquidationorder, "liquidationorderStreamTable", false, true, cacheSize=10000, retentionMinutes=1440);
"""

# 创建 product 流数据表
createProductStreamTableScript = """
colName = ['instType','instId','instFamily','uly','category','baseCcy','quoteCcy','settleCcy','ctVal','ctMult','ctValCcy','optType','stk',
'listTime','expTime','tickSz','lotSz','minSz','ctType','alias','state','maxLmtSz','maxMktSz','maxTwapSz','maxIcebergSz','maxTriggerSz','maxStopSz']
colType = [SYMBOL,SYMBOL,SYMBOL,SYMBOL,STRING,SYMBOL,SYMBOL,SYMBOL,DOUBLE,DOUBLE,SYMBOL,SYMBOL,DOUBLE,
LONG,LONG,DOUBLE,DOUBLE,DOUBLE,SYMBOL,SYMBOL,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE]
product = streamTable(1000:0, colName, colType)
enableTableShareAndPersistence(product, "productStreamTable", false, true, cacheSize=1000, retentionMinutes=1440);
"""

# 创建 status 流数据表
createStatusStreamTableScript = """
colName = ['begin','end','env','href','maintType','preOpenBegin','scheDesc','serviceType','state','system','title','ts']
colType = [LONG,LONG,LONG,STRING,LONG,STRING,STRING,LONG,SYMBOL,SYMBOL,STRING,LONG]
status = streamTable(1000:0, colName, colType)
enableTableShareAndPersistence(status, "statusStreamTable", false, true, cacheSize=1000, retentionMinutes=1440);
"""


# ************ 2.私有频道-流数据表 ************
createShareAccountTableScript = """
colName =[
'availBal','availEq','borrowFroz','cashBal','ccy','coinUsdPrice','crossLiab','disEq','eq','eqUsd',
'fixedBal','frozenBal','interest','isoEq','isoLiab','isoUpl','liab','maxLoan','mgnRatio','notionalLever',
'ordFrozen','spotInUseAmt','stgyEq','twap','uTime','upl','uplLiab']

colType = [
DOUBLE,DOUBLE,DOUBLE,DOUBLE,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,
DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,
DOUBLE,DOUBLE,DOUBLE,DOUBLE,TIMESTAMP,DOUBLE,DOUBLE]

accountTable = latestIndexedTable(`ccy, `uTime, 500:0, colName, colType)
share accountTable as shareAccountTable
"""

# 持仓-内存数据表
createSharePositionTableScript = """
colName = [
'adl','availPos','avgPx','baseBal','baseBorrowed','baseInterest','bizRefId','bizRefType','cTime','ccy',
'closeOrderAlgo','deltaBS','deltaPA','fee','fundingFee','gammaBS','gammaPA','idxPx','imr','instId',
'instType','interest','last','lever','liab','liabCcy','liqPenalty','liqPx','margin','markPx',
'mgnMode','mgnRatio','mmr','notionalUsd','optVal','pTime','pendingCloseOrdLiabVal','pnl','pos','posCcy',
'posId','posSide','quoteBal','quoteBorrowed','quoteInterest','realizedPnl','spotInUseAmt','spotInUseCcy','thetaBS','thetaPA',
'tradeId','uTime','upl','uplLastPx','uplRatio','uplRatioLastPx','usdPx','vegaBS','vegaPA']

colType = [
INT,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,TIMESTAMP,SYMBOL,
DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,SYMBOL,
SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,
SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,TIMESTAMP,DOUBLE,DOUBLE,DOUBLE,DOUBLE,
LONG,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,
LONG,TIMESTAMP,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE]

positionTable = latestIndexedTable(`instId`posSide, `pTime, 5000:0, colName, colType)
share positionTable as sharePositionTable
"""

# 余额-持仓频道-内存数据表(实际上与上面的shareAccountTable内容重叠)
createShareBalanceTableScript = """
colName = ['cashBal','ccy','uTime']
colType = [DOUBLE,SYMBOL,TIMESTAMP]
balanceTable = latestIndexedTable(`ccy, `uTime, 500:0, colName, colType)
share balanceTable as shareBalanceTable
"""

# 订单 - 流数据表
createOrderStreamTableScript = """
colName = [
'accFillSz','algoClOrdId','algoId','amendResult','amendSource','attachAlgoClOrdId','avgPx','cTime','cancelSource','category',
'ccy','clOrdId','code','execType','fee','feeCcy','fillFee','fillFeeCcy','fillFwdPx','fillMarkPx',
'fillMarkVol','fillNotionalUsd','fillPnl','fillPx','fillPxUsd','fillPxVol','fillSz','fillTime','instId','instType',
'lever','msg','notionalUsd','ordId','ordType','pnl','posSide','px','pxType','pxUsd',
'pxVol','quickMgnType','rebate','rebateCcy','reduceOnly','reqId','side','slOrdPx','slTriggerPx','slTriggerPxType',
'source','state','stpId','stpMode','sz','tag','tdMode','tgtCcy','tpOrdPx','tpTriggerPx',
'tpTriggerPxType','tradeId','uTime']

colType = [
DOUBLE,LONG,LONG,INT,INT,LONG,DOUBLE,TIMESTAMP,INT,SYMBOL,
SYMBOL,LONG,INT,SYMBOL,DOUBLE,SYMBOL,DOUBLE,SYMBOL,DOUBLE,DOUBLE,
DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,TIMESTAMP,SYMBOL,SYMBOL,
INT,STRING,DOUBLE,LONG,SYMBOL,DOUBLE,SYMBOL,DOUBLE,SYMBOL,DOUBLE,
DOUBLE,SYMBOL,DOUBLE,SYMBOL,SYMBOL,LONG,SYMBOL,DOUBLE,DOUBLE,SYMBOL,
INT,SYMBOL,LONG,SYMBOL,DOUBLE,STRING,SYMBOL,SYMBOL,DOUBLE,DOUBLE,
SYMBOL,LONG,TIMESTAMP]

order = streamTable(50000:0, colName, colType)
enableTableShareAndPersistence(order , "orderStreamTable", false, true, cacheSize=200000, retentionMinutes=1440);
"""

# 账户爆仓风险预警频道 - 流数据表
createShareWarningTableScript = """
colName = [
'adl','availPos','avgPx','cTime','ccy','deltaBS','deltaPA','gammaBS','gammaPA','imr',
'instId','instType','interest','last','lever','liab','liabCcy','liqPx','markPx','margin',
'mgnMode','mgnRatio','mmr','notionalUsd','optVal','pTime','pos','posCcy','posId','posSide',
'thetaBS','thetaPA','tradeId','uTime','upl','uplRatio','vegaBS','vegaPA']

colType =  [INT,DOUBLE,DOUBLE,TIMESTAMP,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,
            SYMBOL,SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,SYMBOL,DOUBLE,DOUBLE,DOUBLE,
            SYMBOL,DOUBLE,DOUBLE,DOUBLE,DOUBLE,TIMESTAMP,DOUBLE,SYMBOL,LONG,SYMBOL,
            DOUBLE,DOUBLE,LONG,TIMESTAMP,DOUBLE,DOUBLE,DOUBLE,DOUBLE]

warning = latestIndexedTable(`instId`posSide`posId, `pTime, 5000:0, colName, colType)
share warning as shareWarningTable
"""

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

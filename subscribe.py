import hmac
import json
import time
import base64
import asyncio
import websockets
import pandas as pd
from abc import abstractmethod

from .utils.log import logger
from .utils.method import ts2Timestamp
from .setting import TIMEOUT, OKX_API_KEY_SIMULATE,OKX_PASSPHRASE_SIMULATE,OKX_SECRETE_KEY_SIMULATE,OKX_API_KEY,OKX_PASSPHRASE,OKX_SECRETE_KEY
from .okx.consts import SIMULATE_PUBLIC_URL, SIMULATE_CANDLE_URL, SIMULATE_PRIVATE_URL,REAL_PUBLIC_URL,REAL_CANDLE_URL,REAL_PRIVATE_URL


class Subscribe(object):
    """"公告频道抽象订阅类"""
    def __init__(self, kernel, channels, active=True, interval=5, timeout=TIMEOUT) -> None:
        self.kernel = kernel
        self.channels = channels
        self.active = active
        self.interval = interval
        self.timeout = timeout
        self.flag = self.kernel.flag

    def onError(self, msg, error):
        logger.error(f'<{self.__class__.__name__}>: ERROR:{error}, {msg}')

    def onSubscribe(self, packet):
        event = packet['event']
        if event == 'subscribe':
            channel = packet['arg']['channel']
            if 'instId' in packet['arg']:
                instId = packet['arg']['instId']
                print(f'subscribe channel: {channel}, instId: {instId} successfully!')
            elif 'instType' in packet['arg']:
                instType = packet['arg']['instType']
                print(f'subscribe channel: {channel}, instType: {instType} successfully!')
            else:
                print(f'subscribe channel: {channel} successfully!')

        else:
            code = packet['code']
            msg = packet['msg']
            print(f'subscribe failed, code: {code}, msg: {msg}')

    @abstractmethod
    def onPacket(self, packet):
        """数据包回掉函数"""
        pass

    @abstractmethod
    def run(self):
        """允许订阅频道"""
        pass

class PublicSubscribe(Subscribe):
    """构建一个基于公共频道websockets订阅对象类"""
    def __init__(self, kernel, channels, active=True, interval=5, timeout=TIMEOUT) -> None:
        super().__init__(kernel, channels, active, interval, timeout)
        self.url = REAL_PUBLIC_URL if self.flag=='0' else SIMULATE_PUBLIC_URL
    
    def onPacket(self, packet):
        if 'event' in packet:
            self.onSubscribe(packet)
            return 

        channel = packet['arg']['channel']
        if channel == 'instruments':
            self.onProduct(packet)
        elif channel == 'tickers':
            self.onTick(packet)
        elif channel == 'open-interest':
            self.onOpenInterest(packet)
        elif channel == 'trades':
            self.onTrade(packet)
        elif channel == 'estimated-price':
            self.onEstimatedPrice(packet)
        elif channel == 'mark-price':
            self.onMarkPrice(packet)
        elif channel.startswith('mark-price-candle'):
            self.onMarkPriceCandle(packet)
        elif channel == 'price-limit':
            self.onPriceLimit(packet)
        elif channel == 'books':
            self.onBook(packet)
        elif channel == 'opt-summary':
            self.onOptSummary(packet)
        elif channel == 'funding-rate':
            self.onFundingRate(packet)
        elif channel.startswith('index-candle'):
            self.onIndexCandle(packet)
        elif channel == 'index-tickers':
            self.onIndexTick(packet)
        elif channel == 'status':
            self.onStatus(packet)
        elif channel == 'option-trades':
            self.onOptionTrade(packet)
        elif channel == 'liquidation-orders':
            self.onLiquidationOrder(packet)
        else:
            raise KeyError(channel)

    def onProduct(self, packet):
        if packet['data']:
            data = pd.DataFrame(packet['data'])
            data = data.applymap(lambda x: x if x else 0)
            data['category'] = data['category'].astype(int)
            data[['ctVal','ctMult','stk','tickSz','lotSz','minSz','maxLmtSz','maxMktSz','maxTwapSz','maxIcebergSz','maxTriggerSz','maxStopSz']] = \
                 data[['ctVal','ctMult','stk','tickSz','lotSz','minSz','maxLmtSz','maxMktSz','maxTwapSz','maxIcebergSz','maxTriggerSz','maxStopSz']].astype(float)
            data[['listTime', 'expTime']] = data[['listTime', 'expTime']].applymap(ts2Timestamp)
        
    def onTick(self, packet):
        data = pd.DataFrame(packet['data'])
        data.iloc[:,2:] = data.iloc[:,2:].astype('float64')
        data['ts'] = data['ts'].apply(ts2Timestamp)
        self.kernel.dolphin.data2MemoryTable('tickStreamTable', data)

    def onOpenInterest(self, packet):
        data = packet['data']
        data = [data[0]['instType'], data[0]['instId'], float(data[0]['oi']), float(data[0]['oiCcy']), float(data[0]['ts'])]
        self.kernel.dolphin.data2MemoryTable('openinterestStreamTable', data)

    def onEstimatedPrice(self, packet):
        data = packet['data'][0]
        data = [data['instType'], data['instId'], float(data['settlePx']), int(data['ts'])]
        self.kernel.dolphin.data2MemoryTable('estimatedpriceStreamTable', data)

    def onMarkPrice(self, packet):
        data = packet['data'][0]
        data = [data['instType'], data['instId'], float(data['markPx']), int(data['ts'])]
        self.kernel.dolphin.data2MemoryTable('markpriceStreamTable', data)

    def onMarkPriceCandle(self, packet, confirm=False):
        instId = packet['arg']['instId']
        channel = packet['arg']['channel']
        data = packet['data'][0]
        data = [instId,channel,int(data[0]),float(data[1]),float(data[2]),float(data[3]),float(data[4]),int(data[5])]
        if (confirm and int(data[-1]) or not confirm):
            self.kernel.dolphin.data2MemoryTable('markpricecandleStreamTable',data)

    def onPriceLimit(self, packet):
        data = packet['data'][0]
        data = [data['instId'], float(data['buyLmt']), float(data['sellLmt']), int(data['ts'])]
        self.kernel.dolphin.data2MemoryTable('pricelimitStreamTable', data)
    
    @abstractmethod
    def onBook(self, packet):
        pass 

    def onOptSummary(self, packet):
        df = pd.DataFrame(packet['data'])
        df['realVol'] = 0.0
        df.iloc[:,3:16] = df.iloc[:,3:16].astype(float)
        df['fwdPx'] = df['fwdPx'].astype(float)
        df['ts'] = df['ts'].astype('int64')
        self.kernel.dolphin.data2MemoryTable('optsummaryStreamTable', df)

    def onFundingRate(self, packet):
        data = packet['data'][0]
        data = [float(data['fundingRate']), int(data['fundingTime']), data['instId'], data['instType'], float(data['nextFundingRate']), int(data['nextFundingTime'])]
        self.kernel.dolphin.data2MemoryTable('fundingrateStreamTable', data)

    def onIndexCandle(self, packet, confirm=True):
        data = packet['data'][0]
        data = [int(data[0]),float(data[1]),float(data[2]),float(data[3]),float(data[4]),int(data[5])]
        instId = packet['arg']['instId']
        channel = packet['arg']['channel']
        data.extend([instId, channel])
        if (confirm and int(data[5])) or not confirm:
            self.kernel.dolphin.data2MemoryTable('indexcandleStreamTable', data)

    def onIndexTick(self, packet):
        data = packet['data'][0]
        data = [data['instId'],float(data['idxPx']),float(data['high24h']),float(data['low24h']),
                float(data['open24h']),float(data['sodUtc0']),float(data['sodUtc8']),int(data['ts'])]
        self.kernel.dolphin.data2MemoryTable('indextickStreamTable', data)

    def onStatus(self, packet):
        data = packet['data'][0]
        data = [int(data['begin']),int(data['end']),int(data['env']),data['href'],int(data['maintType']),data['preOpenBegin'],
                data['scheDesc'],int(data['serviceType']),data['state'], data['system'], data['title'], int(data['ts'])]
        self.kernel.dolphin.data2MemoryTable('statusStreamTable', data)

    def onOptionTrade(self, packet):
        data = packet['data'][0]
        data = [float(data['fillVol']), float(data['fwdPx']), float(data['indexPx']), data['instFamily'], data['instId'], float(data['markPx']),
                data['optType'], float(data['px']), data['side'], float(data['sz']), int(data['tradeId']), int(data['ts'])]
        self.kernel.dolphin.data2MemoryTable('optiontradeStreamTable', data)

    def onLiquidationOrder(self, packet):
        details = packet['data'][0]['details'][0]
        instId = packet['data'][0]['instId']
        instType = packet['data'][0]['instType']
        uly = packet['data'][0]['uly']
        data = [float(details['bkLoss']),float(details['bkPx']),details['ccy'],details['posSide'],details['side'],float(details['sz']),int(details['ts']),instId,instType,uly]
        self.kernel.dolphin.data2MemoryTable('liquidationorderStreamTable', data)

    async def run(self):
        """异步调用websockets订阅数据"""
        while self.active:
            try:
                async with websockets.connect(self.url) as ws:
                    param = json.dumps({"op": "subscribe", "args": self.channels})
                    await ws.send(param)
                    while self.active:
                        response = await asyncio.wait_for(ws.recv(), timeout=self.timeout)
                        packet = eval(response)
                        self.onPacket(packet)

            except Exception as e:
                self.onError('连接中断, 尝试重连....', e)
                await asyncio.sleep(self.interval)
                continue

class CandleSubscribe(Subscribe):
    """ K线行情订阅公共频道 """
    def __init__(self, kernel, channels, active=True, interval=5, timeout=TIMEOUT) -> None:
        super().__init__(kernel, channels, active, interval, timeout)
        self.url = REAL_CANDLE_URL if self.flag=='0' else SIMULATE_CANDLE_URL
    
    def onPacket(self, packet):
        if 'event' in packet:
            self.onSubscribe(packet)
            return 
        channel = packet['arg']['channel']
        if channel.startswith('candle'):
            self.onCandle(packet)
        else:
            raise KeyError(channel)

    def onCandle(self, packet, confirm=True):
        data = packet['data'][0]
        data = list(map(float, data))
        data[0] = ts2Timestamp(data[0])
        instId = packet['arg']['instId']
        channel = packet['arg']['channel']
        data.extend([instId, channel])
        if (confirm and int(data[-3])) or not confirm:
            self.kernel.dolphin.data2MemoryTable('candleStreamTable', data)

    async def run(self):
        """异步调用websockets订阅数据"""
        while self.active:
            try:
                async with websockets.connect(self.url) as ws:
                    param = json.dumps({"op": "subscribe", "args": self.channels})
                    await ws.send(param)
                    while self.active:
                        response = await asyncio.wait_for(ws.recv(), timeout=self.timeout)
                        packet = eval(response)
                        self.onPacket(packet)

            except Exception as e:
                self.onError('连接中断, 尝试重连....', e)
                await asyncio.sleep(self.interval)
                continue

class PrivateSubscribe(Subscribe):
    """okx 私有频道websocket订阅管理类"""
    def __init__(self, kernel, channels, active=True, interval=5, timeout=TIMEOUT) -> None:
        super().__init__(kernel,channels,active,interval,timeout)
        self.url = REAL_PRIVATE_URL if self.flag=='0' else SIMULATE_PRIVATE_URL
        self.apiKey = OKX_API_KEY if self.flag=='0' else OKX_API_KEY_SIMULATE
        self.passPhrase = OKX_PASSPHRASE if self.flag=='0' else OKX_PASSPHRASE_SIMULATE
        self.secretKey = OKX_SECRETE_KEY if self.flag=='0' else OKX_SECRETE_KEY_SIMULATE

    def onPacket(self, packet):
        if 'event' in packet:
            self.onSubscribe(packet)
            return 
        channel = packet['arg']['channel']      
        if channel == 'account':
            self.onAccount(packet)
        elif channel == 'positions':
            self.onPosition(packet)
        elif channel == 'balance_and_position':
            self.onBalance(packet)
        elif channel == 'orders':
            self.onOrder(packet)
        elif channel == 'liquidation-warning':
            self.onWarning(packet)

    @property
    def loginParam(self):
        timestamp = str(int(time.time()))
        message = timestamp + 'GET' + '/users/self/verify'    
        mac = hmac.new(bytes(self.secretKey, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        digest = mac.digest()
        sign = base64.b64encode(digest)
        loginParamDict = {"op": "login", "args": [{"apiKey": self.apiKey,"passphrase": self.passPhrase,"timestamp": timestamp,"sign": sign.decode("utf-8")}]}
        loginParamStr = json.dumps(loginParamDict)
        return loginParamStr

    def onLogin(self, packet):
        msg = packet['msg']
        if packet['code'] == '0':
            logger.info('<PrivateSubscribe>: Login successfully!')
        else:
            logger.info(f'<PrivateSubscribe>: Login failed for code: {msg}!')

    def onAccount(self, packet):
        details = packet['data'][0]['details']
        data = pd.DataFrame(details)
        data = data.applymap(lambda x: 0 if x=='' else x) # 解决值为：'' 的情况
        ccy = data['ccy']
        data['ccy'] = 0
        data = data.astype(float)
        data['ccy'] = ccy
        data['uTime'] = data['uTime'].apply(ts2Timestamp)
        self.kernel.dolphin.data2MemoryTable('shareAccountTable', data)

    def onPosition(self, packet):
        if packet['data']:
            data = pd.DataFrame(packet['data'])
            data = data.applymap(lambda x: x if x else 0)
            data['adl'] = data['adl'].astype(int)
            data[['posId', 'tradeId']] = data[['posId', 'tradeId']].astype('int64')
            data[['availPos','avgPx','baseBal','baseBorrowed','baseInterest','bizRefId','bizRefType','closeOrderAlgo',
                'deltaBS','deltaPA','fee','fundingFee','gammaBS','gammaPA','idxPx','imr','interest','last','lever','liab','liabCcy',
                'liqPenalty','liqPx','margin','markPx','mgnRatio','mmr','notionalUsd','optVal','pendingCloseOrdLiabVal','pnl','pos',
                'posCcy','quoteBal','quoteBorrowed','quoteInterest','realizedPnl','spotInUseAmt','spotInUseCcy','thetaBS','thetaPA',
                'upl','uplLastPx','uplRatio','uplRatioLastPx','usdPx','vegaBS','vegaPA']] = \
                data[['availPos','avgPx','baseBal','baseBorrowed','baseInterest','bizRefId','bizRefType','closeOrderAlgo',
                'deltaBS','deltaPA','fee','fundingFee','gammaBS','gammaPA','idxPx','imr','interest','last','lever','liab','liabCcy',
                'liqPenalty','liqPx','margin','markPx','mgnRatio','mmr','notionalUsd','optVal','pendingCloseOrdLiabVal','pnl','pos',
                'posCcy','quoteBal','quoteBorrowed','quoteInterest','realizedPnl','spotInUseAmt','spotInUseCcy','thetaBS','thetaPA',
                'upl','uplLastPx','uplRatio','uplRatioLastPx','usdPx','vegaBS','vegaPA']].astype('float64')
            data[['cTime', 'pTime', 'uTime']] = data[['cTime', 'pTime', 'uTime']].applymap(ts2Timestamp)
            self.kernel.dolphin.data2MemoryTable('sharePositionTable', data)
            
    def onBalance(self, packet):
        balData = packet['data'][0]['balData']
        data = pd.DataFrame(balData)
        data['cashBal'] = data['cashBal'].astype(float)
        data['uTime'] = data['uTime'].apply(ts2Timestamp)
        self.kernel.dolphin.data2MemoryTable('shareBalanceTable', data)

    def onOrder(self, packet):
        if packet['data']:
            data = pd.DataFrame(packet['data'])
            data[['amendResult', 'amendSource', 'cancelSource', 'code', 'lever', 'source','algoClOrdId','algoId','attachAlgoClOrdId','clOrdId','ordId','reqId','stpId','tradeId']] = \
                data[['amendResult', 'amendSource', 'cancelSource', 'code', 'lever', 'source','algoClOrdId','algoId','attachAlgoClOrdId','clOrdId','ordId','reqId','stpId','tradeId']].applymap(lambda x:int(x) if x!='' else 0)
            data[['accFillSz','avgPx','fee','fillFee','fillNotionalUsd','fillPnl','fillPx','fillSz','notionalUsd','pnl','px','rebate','slOrdPx','slTriggerPx','sz','tpOrdPx','tpTriggerPx','fillFwdPx','fillMarkPx','fillMarkVol','fillPxUsd','fillPxVol','pxUsd','pxVol']] = \
                data[['accFillSz','avgPx','fee','fillFee','fillNotionalUsd','fillPnl','fillPx','fillSz','notionalUsd','pnl','px','rebate','slOrdPx','slTriggerPx','sz','tpOrdPx','tpTriggerPx','fillFwdPx','fillMarkPx','fillMarkVol','fillPxUsd','fillPxVol','pxUsd','pxVol']].applymap(lambda x:float(x) if x!='' else 0)
            data[['cTime', 'fillTime', 'uTime']] = data[['cTime', 'fillTime', 'uTime']].applymap(ts2Timestamp)
            self.kernel.dolphin.data2MemoryTable('orderStreamTable', data)

    def onTrade(self, packet):
        data = packet['data']
        data = [data[0]['instId'], int(data[0]['tradeId']), float(data[0]['px']), float(data[0]['sz']), data[0]['side'], int(data[0]['ts'])]
        self.kernel.dolphin.data2MemoryTable('tradeStreamTable', data)
    
    def onWarning(self, packet):
        print(packet)
        if packet['data']:
            data = pd.DataFrame(packet['data'])
            data['adl'] = data['adl'].astype(int)
            data[['posId', 'tradeId']] = data[['posId', 'tradeId']].astype('int64')
            data[['availPos','avgPx','deltaBS', 'deltaPA','gammaBS','gammaPA','imr','interest',
                  'last','lever','liab','liqPx','markPx','margin','mgnRatio','mmr','notionalUsd',
                  'optVal','pos','thetaBS','thetaPA','upl','uplRatio','vegaBS','vegaPA']] = \
                  data[['availPos','avgPx','deltaBS', 'deltaPA','gammaBS','gammaPA','imr','interest',
                  'last','lever','liab','liqPx','markPx','margin','mgnRatio','mmr','notionalUsd',
                  'optVal','pos','thetaBS','thetaPA','upl','uplRatio','vegaBS','vegaPA']].astype(float)
            data[['cTime', 'pTime', 'uTime']] = data[['cTime', 'pTime', 'uTime']].applymap(ts2Timestamp)
            self.kernel.dolphin.data2MemoryTable('shareWarningTable', data)

    async def run(self):
        while self.active:
            try:
                async with websockets.connect(self.url) as ws:
                    # login
                    await ws.send(self.loginParam)
                    response = await ws.recv()
                    packet = eval(response)
                    self.onLogin(packet)

                    # subscribe
                    param = json.dumps({"op": "subscribe", "args": self.channels})
                    await ws.send(param)
                    while self.active:
                        response = await asyncio.wait_for(ws.recv(), timeout=self.timeout)
                        packet = eval(response)
                        self.onPacket(packet)

            except Exception as e:
                self.onError('连接中断, 尝试重连....', e)
                await asyncio.sleep(self.interval)
                continue
        

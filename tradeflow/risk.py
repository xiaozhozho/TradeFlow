from .setting import LEVER, MAX_LOSS, MAX_PROFIT


class RiskManage:
    """ 风险管理模块"""
    def __init__(self, kernel, posMode='net_mode', mgnMode='cross', lever=LEVER, maxLoss=MAX_LOSS, maxProfit=MAX_PROFIT, pause=1) -> None:
        self.kernel = kernel 
        self.posMode = posMode
        self.mgnMode = mgnMode
        self.lever = lever
        self.maxLoss = maxLoss 
        self.maxProfit = maxProfit
        self.pause = pause
        self._active = True

    def log(self, msg) -> None:
        self.kernel.logger.info(f'<Risk>: {msg}')   

    def run(self):
        """ 轮询查询账户、持仓风险, 实施全局风险管理(目前只有绝对止损规则) """
        lastPosition = self.kernel.getLastPositions()
        for instId in lastPosition.keys():
            # 调节杠杆
            lever = lastPosition.get(instId,{}).get('lever', 0)
            pos = lastPosition.get(instId,{}).get('pos', 0)
            uplRatio = lastPosition.get(instId,{}).get('uplRatio',0)
            if lever!=self.lever and pos!=0:
                self.kernel.api.account.set_leverage(self.lever, mgnMode='cross', instId=instId)
                
            # 止损止盈
            if uplRatio < -self.maxLoss and pos!=0:
                try:
                    packet = self.kernel.api.trade.close_positions(instId, lastPosition[instId]['mgnMode'], lastPosition[instId]['posSide'], ccy=lastPosition[instId]['ccy'])
                    if packet['code'] == '0':
                        self.log(f"stop {instId} loss success, loss ratio {round(lastPosition[instId]['uplRatio']*100,4)}%.")
                    else:
                        self.log(f"stop {instId} loss failure for error code {packet['code']}.")
                except Exception as e:
                    self.log(e)
                
            elif uplRatio > self.maxProfit and pos!=0:
                try:
                    packet = self.kernel.api.trade.close_positions(instId, lastPosition[instId]['mgnMode'], lastPosition[instId]['posSide'], ccy=lastPosition[instId]['ccy'])
                    if packet['code'] == '0':
                        self.log(f"stop {instId} profit success, profit ratio {round(lastPosition[instId]['uplRatio']*100,4)}%.")
                    else:
                        self.log(f"stop {instId} profit failure for error code {packet['code']}.") 
                except Exception as e:
                    self.log(e)
                
            else: continue

        


            



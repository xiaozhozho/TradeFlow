import time

from .setting import LEVER, MAX_LOSS, MAX_PROFIT
from .utils.format import as_float


class RiskManage:
    """ 风险管理模块：杠杆校正 + 绝对止损/止盈"""
    lever_retry_seconds = 30  # 杠杆校正的重试间隔（秒），避免失败时逐秒刷 REST

    def __init__(self, kernel, posMode='net_mode', mgnMode='cross', lever=LEVER, maxLoss=MAX_LOSS, maxProfit=MAX_PROFIT, pause=1) -> None:
        self.kernel = kernel
        self.posMode = posMode
        self.mgnMode = mgnMode
        self.lever = lever
        self.maxLoss = maxLoss
        self.maxProfit = maxProfit
        self.pause = pause
        self._active = True
        # 已发出平仓请求、等待仓位消失的 (instId, posSide) 集合，避免逐秒重复发单
        self._closing = set()
        # {instId: monotonic 到期时间}：杠杆校正退避窗口
        self._lever_retry_at = {}

    def log(self, msg) -> None:
        self.kernel.logger.info(f'<Risk>: {msg}')   

    def run(self):
        """ 轮询查询账户、持仓风险, 实施全局风险管理(目前只有绝对止损规则) """
        for position in self.kernel.getPositions():
            instId = position.get('instId', '')
            posSide = position.get('posSide') or 'net'
            key = (instId, posSide)
            pos = as_float(position.get('pos'))
            uplRatio = as_float(position.get('uplRatio'))
            if pos == 0:
                # 仓位已消失，解除平仓抑制
                self._closing.discard(key)
                continue
            # 调节杠杆（带退避：同一合约 lever_retry_seconds 内只尝试一次）
            lever = as_float(position.get('lever'))
            if lever != self.lever:
                now = time.monotonic()
                if now >= self._lever_retry_at.get(instId, 0.0):
                    self._lever_retry_at[instId] = now + self.lever_retry_seconds
                    try:
                        self.kernel.api.account.set_leverage(self.lever, mgnMode='cross', instId=instId)
                    except Exception as e:
                        self.log(f'lever {instId}: {e}')

            if key in self._closing:
                # 平仓请求已发出，等待仓位归零，不再重复发单
                continue

            if uplRatio < -self.maxLoss or uplRatio > self.maxProfit:
                action = 'loss' if uplRatio < -self.maxLoss else 'profit'
                self._closing.add(key)
                try:
                    packet = self.kernel.api.trade.close_positions(
                        instId, position.get('mgnMode', self.mgnMode), posSide,
                        ccy=position.get('ccy', ''),
                    )
                    if packet.get('code') == '0':
                        self.log(f"stop {key} {action} success, uplRatio {round(uplRatio * 100, 4)}%.")
                    else:
                        self.log(f"stop {key} {action} failure for error code {packet.get('code')}.")
                        self._closing.discard(key)  # 失败下次轮询重试
                except Exception as e:
                    self.log(e)
                    self._closing.discard(key)

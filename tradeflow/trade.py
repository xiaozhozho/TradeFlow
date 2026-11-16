"""策略交易便捷层（薄封装，不替代 okx SDK / kernel.api）。

职责：
- 下单数量按 ``lotSz`` 向下取整、价格按 ``tickSz`` 四舍五入，
  并校验最小下单量（``minSz``），避免 OKX 拒单；
- 自动生成 ``clOrdId``，便于在 orders 频道与快照表中关联订单；
- 合约规格优先读 DolphinDB ``productStreamTable``（instruments 频道），
  未命中时回退 OKX REST ``get_instruments`` 并缓存。

返回值为 OKX REST 原始应答（``{"code", "msg", "data"}``），
``code == "0"`` 表示请求被接受。
"""
from __future__ import annotations

import random
import time
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP


def _inst_type(inst_id: str) -> str:
    """BTC-USDT-SWAP -> SWAP；BTC-USDT-250328 -> FUTURES；其余按 SPOT。"""
    if inst_id.endswith("-SWAP"):
        return "SWAP"
    parts = inst_id.split("-")
    if len(parts) == 3 and parts[-1].isdigit():
        return "FUTURES"
    return "SPOT"


class Trader:
    """交易便捷层：合约规格规整 + 下单/平仓/查询。"""

    def __init__(self, kernel):
        self.kernel = kernel
        self._specs: dict[str, dict] = {}

    # ---------- 合约规格 ----------

    def symbolInfo(self, instId: str) -> dict:
        """{lotSz, minSz, tickSz}：先查流表快照，再回退 REST，结果缓存。"""
        if instId in self._specs:
            return self._specs[instId]
        spec = self._symbol_from_db(instId) or self._symbol_from_rest(instId)
        if spec is None:
            raise RuntimeError(f"无法获取 {instId} 的合约规格（lotSz/minSz/tickSz）")
        self._specs[instId] = spec
        return spec

    def _symbol_from_db(self, instId: str):
        try:
            frame = self.kernel.dolphin.runScript(
                f"select top 1 lotSz, minSz, tickSz, ctVal from productStreamTable "
                f"where instId='{instId}'"
            )
        except Exception:
            return None
        if frame is None or len(frame) == 0:
            return None
        row = frame.iloc[0]
        return {
            "lotSz": str(row["lotSz"]),
            "minSz": str(row["minSz"]),
            "tickSz": str(row["tickSz"]),
            "ctVal": str(row.get("ctVal", 0)),
        }

    def _symbol_from_rest(self, instId: str):
        try:
            packet = self.kernel.api.public.get_instruments(_inst_type(instId), instId=instId)
        except Exception:
            return None
        if packet.get("code") != "0":
            return None
        data = packet.get("data") or []
        if not data:
            return None
        item = data[0]
        return {
            "lotSz": str(item.get("lotSz", "1")),
            "minSz": str(item.get("minSz", "0")),
            "tickSz": str(item.get("tickSz", "0")),
            "ctVal": str(item.get("ctVal", "0")),
        }

    # ---------- 规划 ----------

    @staticmethod
    def roundSize(instId: str, sz, spec: dict) -> str:
        """按 lotSz 向下取整并校验 minSz，返回字符串数量。"""
        lot = Decimal(str(spec.get("lotSz") or "1"))
        min_sz = Decimal(str(spec.get("minSz") or "0"))
        value = Decimal(str(sz))
        if lot > 0:
            value = (value / lot).to_integral_value(rounding=ROUND_DOWN) * lot
        if value <= 0 or value < min_sz:
            raise ValueError(
                f"{instId} 下单数量 {sz} 经步长规整后为 {value}，低于最小下单量 {min_sz}"
            )
        return format(value, "f")

    @staticmethod
    def roundPrice(px, spec: dict) -> str:
        """按 tickSz 四舍五入到合法价位，返回字符串价格。"""
        tick = Decimal(str(spec.get("tickSz") or "0"))
        value = Decimal(str(px))
        if tick > 0:
            value = (value / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick
        return format(value, "f")

    @staticmethod
    def makeClOrdId() -> str:
        """生成 OKX 允许的字母数字 clOrdId（<=32 字符），便于订单关联。"""
        return f"tf{int(time.time() * 1000)}{random.randint(1000, 9999)}"

    # ---------- 保证金换算与限额 ----------

    def lastPrice(self, instId: str) -> float:
        """最新成交价（shareTickTable 快照）。"""
        frame = self.kernel.dolphin.runScript(
            f"select * from shareTickTable where instId='{instId}'"
        )
        if frame is None or frame.empty:
            raise ValueError(f"无 {instId} 的 tick 快照，无法定价")
        price = float(frame.iloc[0]["last"] or 0)
        if price <= 0:
            raise ValueError(f"{instId} 最新价无效: {price}")
        return price

    def sizeByMargin(self, instId: str, margin_usdt, price=None) -> float:
        """保证金 -> 张数：margin x 杠杆 / (价格 x 合约面值)；含下单前限额校验。"""
        spec = self.symbolInfo(instId)
        ct_val = float(spec.get("ctVal") or 0)
        if ct_val <= 0:
            raise ValueError(f"{instId} 缺少合约面值 ctVal")
        price = float(price) if price else self.lastPrice(instId)
        leverage = float(getattr(self.kernel.risk, "lever", 1) or 1)
        margin = float(margin_usdt)
        self._check_margin_limits(instId, margin, price, ct_val, leverage)
        return margin * leverage / (price * ct_val)

    def _check_margin_limits(self, instId, margin, price, ct_val, leverage):
        """下单前限额：单笔保证金上限 + 单品种持仓保证金上限。"""
        max_order = getattr(self.kernel, "max_order_margin", None)
        if max_order is not None and margin > max_order:
            raise ValueError(f"单笔保证金 {margin:g}U 超过上限 {max_order:g}U")
        max_position = getattr(self.kernel, "max_position_margin", None)
        if max_position is not None:
            position = self.getPosition(instId) or {}
            current = abs(float(position.get("pos") or 0))
            used = current * ct_val * price / leverage if leverage else 0.0
            if used + margin > max_position:
                raise ValueError(
                    f"持仓保证金 {used:.2f}U + 本单 {margin:g}U 超过上限 {max_position:g}U"
                )

    # ---------- 下单 ----------

    def buy(self, instId: str, sz, px=None, ordType: str = "market",
            tdMode: str = "cross", posSide: str = "", reduceOnly: bool = False):
        """买入；market 单忽略 px，limit 单必须提供 px。"""
        return self._place("buy", instId, sz, px, ordType, tdMode, posSide, reduceOnly)

    def sell(self, instId: str, sz, px=None, ordType: str = "market",
             tdMode: str = "cross", posSide: str = "", reduceOnly: bool = False):
        """卖出；reduceOnly=True 表示只减仓。"""
        return self._place("sell", instId, sz, px, ordType, tdMode, posSide, reduceOnly)

    def _place(self, side, instId, sz, px, ordType, tdMode, posSide, reduceOnly):
        spec = self.symbolInfo(instId)
        size = self.roundSize(instId, sz, spec)
        price = ""
        if ordType == "limit":
            if px is None:
                raise ValueError("限价单需要提供 px")
            price = self.roundPrice(px, spec)
        packet = self.kernel.api.trade.place_order(
            instId=instId, tdMode=tdMode, side=side, ordType=ordType,
            sz=size, px=price, clOrdId=self.makeClOrdId(),
            posSide=posSide, reduceOnly="true" if reduceOnly else "",
        )
        return packet

    # ---------- 批量与撤单 ----------

    def batchBuy(self, orders: list) -> list:
        """批量买入：orders=[{"instId", "sz", "px"?, "ordType"?, "tdMode"?, "posSide"?, "reduceOnly"?}]。"""
        return self._place_batch("buy", orders)

    def batchSell(self, orders: list) -> list:
        """批量卖出，字段同 batchBuy。"""
        return self._place_batch("sell", orders)

    def _place_batch(self, side, orders):
        payload = []
        for order in orders:
            instId = order["instId"]
            spec = self.symbolInfo(instId)
            ordType = order.get("ordType", "market")
            price = ""
            if ordType == "limit":
                if order.get("px") is None:
                    raise ValueError(f"{instId} 限价单需要提供 px")
                price = self.roundPrice(order["px"], spec)
            payload.append({
                "instId": instId,
                "tdMode": order.get("tdMode", "cross"),
                "side": side,
                "ordType": ordType,
                "sz": self.roundSize(instId, order["sz"], spec),
                "px": price,
                "clOrdId": order.get("clOrdId") or self.makeClOrdId(),
                "posSide": order.get("posSide", ""),
                "reduceOnly": "true" if order.get("reduceOnly") else "",
            })
        packets = []
        for start in range(0, len(payload), 20):  # OKX 单次批量上限 20
            packets.append(
                self.kernel.api.trade.place_multiple_orders(payload[start:start + 20])
            )
        return packets[0] if len(packets) == 1 else packets

    def cancel(self, instId: str, ordId: str = "", clOrdId: str = ""):
        """撤单（ordId 或 clOrdId 二选一）。"""
        return self.kernel.api.trade.cancel_order(instId, ordId=ordId, clOrdId=clOrdId)

    # ---------- 平仓与查询 ----------

    def close(self, instId: str, mgnMode: str = "cross", posSide: str = "", ccy: str = ""):
        """按仓位快照平仓；快照缺失时按传入参数平仓（双向持仓逐个平）。"""
        rows = self.getPositions(instId) or [{}]
        packets = [
            self.kernel.api.trade.close_positions(
                instId,
                row.get("mgnMode") or mgnMode,
                posSide or row.get("posSide") or "",
                ccy=ccy or row.get("ccy") or "",
            )
            for row in rows
        ]
        return packets[0] if len(packets) == 1 else packets

    def getPositions(self, instId: str | None = None) -> list:
        """持仓快照列表（sharePositionTable），instId=None 返回全部。"""
        frame = self.kernel.dolphin.runScript("select * from sharePositionTable")
        if frame is None or frame.empty:
            return []
        records = frame.to_dict("records")
        if instId:
            records = [row for row in records if row.get("instId") == instId]
        return records

    def getPosition(self, instId: str):
        """单品种持仓快照（无持仓返回 None）。"""
        rows = self.getPositions(instId)
        return rows[0] if rows else None

    def getAccount(self, ccy: str | None = None):
        """账户资金快照；ccy=None 返回 {ccy: 字段字典} 全量。"""
        account = self.kernel.getLastAccount()
        return account.get(ccy) if ccy else account

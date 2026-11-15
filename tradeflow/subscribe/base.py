"""WebSocket 连接基类：只负责收包、心跳、重连与动态订阅。

数据解析交给 ``handlers.dispatch``，落库交给 ``StreamWriter``；
本类对具体频道无感知，公共/业务/私有连接共用同一套连接管理。

心跳策略（OKX 要求客户端主动保活）：
- 每 ``WS_HEARTBEAT_INTERVAL`` 秒无条件发送 "ping"；
- 服务端回 "pong"，收到即刷新 ``_last_message_at``；
- ``recv`` 超时（``WS_RECV_TIMEOUT``）视为连接失效，触发重连。
"""
from __future__ import annotations

import asyncio
import json
import time

import websockets

from ..setting import (
    WS_HEARTBEAT_INTERVAL,
    WS_RECV_TIMEOUT,
    WS_RECONNECT_MAX,
)
from ..utils.log import get_logger
from .handlers import dispatch

logger = get_logger()

# 连接级错误码：这类错误重订阅无效，必须重建连接
CONNECTION_ERROR_CODES = {"60005", "60006", "60007", "60009", "60010", "60011"}


class Subscribe:
    """OKX WebSocket 订阅基类。"""

    endpoint = "public"
    login_required = False

    def __init__(self, kernel, channels, active=True, interval=5,
                 timeout=WS_RECV_TIMEOUT, writer=None, reconciler=None, url=None):
        self.kernel = kernel
        self.active = active
        self.interval = interval
        self.timeout = timeout
        self.writer = writer
        self.reconciler = reconciler
        self.flag = getattr(kernel, "flag", "0")
        self.url = url or self.default_url()
        self._desired = {self._key(arg): arg for arg in (channels or [])}
        self._ws = None
        self._connected = False
        self._last_message_at = 0.0
        self._reconnect_count = 0
        self._error_count = 0
        self._request_count = 0
        self._send_lock = asyncio.Lock()
        self._heartbeat_task: asyncio.Task | None = None

    # ---------- 子类必须提供 ----------

    def default_url(self) -> str:
        raise NotImplementedError

    @property
    def loginParam(self) -> str:
        raise NotImplementedError("login_required=True 的连接必须实现 loginParam")

    # ---------- 频道集合管理 ----------

    @staticmethod
    def _key(arg: dict) -> str:
        return json.dumps(arg, sort_keys=True, ensure_ascii=False)

    @property
    def desired(self) -> dict:
        return self._desired

    @property
    def error_count(self) -> int:
        return self._error_count

    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    def add_desired(self, args) -> None:
        for arg in args:
            self._desired[self._key(arg)] = arg

    def remove_desired(self, args) -> None:
        for arg in args:
            self._desired.pop(self._key(arg), None)

    async def subscribe(self, args) -> None:
        """动态订阅：只发送未包含的频道。"""
        fresh = []
        for arg in args:
            key = self._key(arg)
            if key not in self._desired:
                self._desired[key] = arg
                fresh.append(arg)
        if fresh and self._ws is not None and self._connected:
            await self._send_json({"op": "subscribe", "args": fresh})
            self._request_count += 1

    async def unsubscribe(self, args) -> None:
        removed = []
        for arg in args:
            key = self._key(arg)
            if self._desired.pop(key, None) is not None:
                removed.append(arg)
        if removed and self._ws is not None and self._connected:
            await self._send_json({"op": "unsubscribe", "args": removed})
            self._request_count += 1

    # ---------- 连接生命周期 ----------

    async def run(self):
        """重连主循环：指数退避，直到 active=False。"""
        backoff = self.interval
        try:
            while self.active:
                try:
                    await self._connect()
                    backoff = self.interval
                    await self._consume()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._error_count += 1
                    self._reconnect_count += 1
                    self.onError("连接中断, 尝试重连", exc)
                    await self._close()
                    if not self.active:
                        break
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, WS_RECONNECT_MAX)
        finally:
            await self._close()

    async def _connect(self):
        self._ws = await websockets.connect(
            self.url,
            ping_interval=None,   # 用 OKX 的文本 ping/pong，禁用协议层 keepalive
            close_timeout=5,
            max_queue=4096,
        )
        if self.login_required:
            await self._send_text(self.loginParam)
            raw = await asyncio.wait_for(self._ws.recv(), timeout=10)
            packet = json.loads(raw)
            if packet.get("event") != "login" or packet.get("code") != "0":
                raise RuntimeError(f"OKX 登录失败: {packet}")
        if self._desired:
            await self._send_json({"op": "subscribe", "args": list(self._desired.values())})
            self._request_count += 1
        self._connected = True
        self._last_message_at = time.time()
        self._heartbeat_task = asyncio.create_task(self._heartbeat())
        if self.reconciler is not None:
            await asyncio.to_thread(self.reconciler.after_reconnect, self.endpoint)

    async def _consume(self):
        while self.active and self._ws is not None:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=self.timeout)
            self._last_message_at = time.time()
            if raw == "pong":
                continue
            packet = json.loads(raw)
            if "event" in packet:
                self._handle_event(packet)
                continue
            for batch in dispatch(packet):
                if self.writer is not None:
                    self.writer.put(batch.table, batch.frame)

    async def _heartbeat(self):
        while self.active and self._ws is not None:
            await asyncio.sleep(WS_HEARTBEAT_INTERVAL)
            try:
                await self._send_text("ping")
            except Exception:
                return  # 发送失败交给主循环重连

    async def _close(self):
        self._connected = False
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

    # ---------- 收发 ----------

    async def _send_text(self, text: str) -> None:
        async with self._send_lock:
            if self._ws is None:
                raise ConnectionError("连接未建立")
            await self._ws.send(text)

    async def _send_json(self, payload: dict) -> None:
        await self._send_text(json.dumps(payload))

    # ---------- 事件处理 ----------

    def _handle_event(self, packet: dict) -> None:
        event = packet.get("event")
        if event == "error":
            code = str(packet.get("code", ""))
            self._error_count += 1
            logger.error(f"<{self.__class__.__name__}> 频道错误: {packet}")
            arg = packet.get("arg") or {}
            if arg.get("channel"):
                # 单频道错误只摘除该频道，其余频道继续
                self._desired.pop(self._key(arg), None)
            if code in CONNECTION_ERROR_CODES:
                raise RuntimeError(f"OKX 连接级错误 {code}: {packet.get('msg')}")
            return
        if event == "notice":
            logger.warning(f"<{self.__class__.__name__}> notice: {packet}")
            return
        if event in {"channel-conn-count", "channel-conn-count-error"}:
            logger.warning(f"<{self.__class__.__name__}> 连接数通知: {packet}")
            return
        if event in {"subscribe", "unsubscribe", "login"}:
            arg = packet.get("arg", packet.get("msg", ""))
            logger.info(f"<{self.__class__.__name__}> {event}: {arg}")
            return
        logger.warning(f"<{self.__class__.__name__}> 未知事件: {packet}")

    # ---------- 健康与日志 ----------

    def onError(self, msg, error):
        logger.error(f"<{self.__class__.__name__}>: ERROR:{error}, {msg}")

    def health(self) -> dict:
        return {
            "endpoint": self.endpoint,
            "connected": self._connected,
            "last_message_at": self._last_message_at,
            "reconnect_count": self._reconnect_count,
            "error_count": self._error_count,
            "request_count": self._request_count,
            "channels": [
                "{channel}:{id}".format(
                    channel=arg.get("channel"),
                    id=arg.get("instId") or arg.get("instType")
                    or arg.get("instFamily") or arg.get("ccy") or "",
                )
                for arg in self._desired.values()
            ],
        }

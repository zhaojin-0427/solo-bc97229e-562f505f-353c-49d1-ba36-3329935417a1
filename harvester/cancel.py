"""取消令牌：取消受理后立即中断该轮正在接收的 HTTP 响应。

- :meth:`CancelToken.cancel` 由控制台线程调用：置位事件并硬中断在途响应
  （先 shutdown 底层 socket 唤醒阻塞的 recv，再 close），既不等慢正文
  发送完毕，也不等读超时；
- 采集线程在请求前、响应体分块读取中、落盘前分别检查事件，保证取消后
  尽快退出抓取循环，且不再写入本轮数据。
"""
from __future__ import annotations

import threading


class HarvestCancelled(Exception):
    """本轮采集已被取消（控制流异常，不是网络错误，不可续采）。"""


class CancelToken:
    """每源每轮一个取消句柄：取消事件 + 当前在途响应的硬中断能力。"""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._response = None  # 当前正在读取的 requests.Response
        # 引擎在 badResumptionToken 重采推进轮次时同步更新，供注册表比对
        self.round_id: int | None = None

    def is_set(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> None:
        """置位取消事件，并立即中断正在接收的响应（若已登记）。"""
        self._event.set()
        with self._lock:
            resp = self._response
        if resp is not None:
            _hard_interrupt(resp)

    def register(self, resp) -> None:
        """登记在途响应；若取消已发生则立即中断它（消除登记前竞态）。"""
        with self._lock:
            self._response = resp
            pending = self._event.is_set()
        if pending:
            _hard_interrupt(resp)

    def unregister(self, resp) -> None:
        with self._lock:
            if self._response is resp:
                self._response = None


def _hard_interrupt(resp) -> None:
    """硬中断一个 requests 响应：shutdown 读方向唤醒阻塞的 recv，再关闭。"""
    raw = getattr(resp, "raw", None)
    interrupted = False
    if raw is not None and hasattr(raw, "shutdown"):
        try:
            raw.shutdown()  # urllib3>=2：对底层 socket 执行 SHUT_RD
            interrupted = True
        except Exception:
            pass
    if not interrupted:
        # 兜底：直接摸到底层 socket 做全双工 shutdown
        try:
            import socket

            fp = getattr(raw, "_fp", None)
            inner = getattr(fp, "fp", None)
            sock_io = getattr(inner, "raw", None)
            sock = getattr(sock_io, "_sock", None)
            if sock is not None:
                sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
    try:
        resp.close()
    except Exception:
        pass

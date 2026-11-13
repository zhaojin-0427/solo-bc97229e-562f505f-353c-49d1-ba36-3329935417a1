"""采集引擎：每源一个后台线程，同源重复启动复用同一任务。

关键约束：
- 网络请求（等待 OAI 响应）在数据库事务之外进行，不持有任何写事务；
- 每一页拿到结果后开一个短事务落盘记录 + resumptionToken，重启即可续采；
- badResumptionToken 时清空本轮暂存、round 加一从首页重采，正式目录完全不动；
- 仅完整一轮（最后一页无 token）才调用 complete_round 原子替换本来源目录；
- 两源线程独立运行，可并行；同一来源并发启动只复用，不新建；
- 取消：控制台受理后（cancelling 已持久化）立即硬中断该轮在途响应，
  采集线程退出抓取循环、清空本轮暂存与令牌后才置 cancelled；
  取消中拒绝同源启动；清理期间进程重启会在启动时继续完成取消；
  迟到响应与异常收尾一律按轮次 + 状态守卫，不得修改新轮。
"""
from __future__ import annotations

import logging
import threading

from . import repository
from .cancel import CancelToken, HarvestCancelled
from .config import Config
from .db import connect
from .oai import OaiClient, OaiError, OaiNetworkError

log = logging.getLogger("harvester.engine")


class HarvestManager:
    def __init__(self, config: Config):
        self.config = config
        # 控制连接只承担启动/续采/取消的短事务；工作线程各自持有独立连接
        self._control_conn = connect(config.db_path)
        self._threads: dict[int, threading.Thread] = {}
        # source_id -> (round_id, CancelToken)：按轮次登记，迟到的取消不影响新轮
        self._cancel_tokens: dict[int, tuple[int, CancelToken]] = {}
        self._lock = threading.Lock()

    def is_running(self, source_id: int) -> bool:
        with self._lock:
            t = self._threads.get(source_id)
            return bool(t and t.is_alive())

    def start(self, source_id: int) -> str:
        """启动/续采某来源。

        返回 'started' / 'resumed' / 'reused' / 'cancelling'（取消收尾中，拒绝启动）。
        """
        with self._lock:
            existing = self._threads.get(source_id)
            alive = bool(existing and existing.is_alive())
        if alive:
            status = repository.get_state(self._control_conn, source_id)["status"]
            if status == "cancelling":
                return "cancelling"  # 取消收尾期间拒绝同源启动
            if status == "running":
                return "reused"
            # 线程正在做收尾（状态已到终态），等它退出后再开新任务
            existing.join(timeout=5)
        with self._lock:
            existing = self._threads.get(source_id)
            if existing and existing.is_alive():
                return "reused"
            try:
                action, round_id = repository.start_or_resume(
                    self._control_conn, source_id
                )
            except repository.CancelInProgress:
                return "cancelling"
            except repository.AlreadyRunning:
                return "reused"
            token = CancelToken()
            token.round_id = round_id
            self._cancel_tokens[source_id] = (round_id, token)
            t = threading.Thread(
                target=self._run_safely,
                args=(source_id, round_id, token),
                name=f"harvest-src{source_id}",
                daemon=True,
            )
            self._threads[source_id] = t
            t.start()
            return action

    def start_all(self) -> dict[int, str]:
        return {src["id"]: self.start(src["id"]) for src in self.config.sources}

    def cancel(self, source_id: int, round_id: int) -> dict:
        """受理取消：先持久化取消意图，再硬中断该轮在途响应。

        取消与发布互斥由 request_cancel 的事务保证；这里只负责触发中断。
        重复取消返回原结果（幂等）；轮次不匹配直接拒绝，不触碰新轮。
        """
        result = repository.request_cancel(self._control_conn, source_id, round_id)
        if result["result"] in ("accepted", "already_cancelling"):
            with self._lock:
                cur = self._cancel_tokens.get(source_id)
            # 只中断与请求轮次一致的任务：迟到的取消不得影响新轮
            if cur is not None and cur[0] == round_id:
                cur[1].cancel()
        return result

    def _note_round(
        self, source_id: int, round_id: int, token: CancelToken
    ) -> None:
        """badToken 重采推进轮次后，同步更新取消注册表（同一令牌延续）。"""
        with self._lock:
            cur = self._cancel_tokens.get(source_id)
            if cur is not None and cur[1] is token:
                self._cancel_tokens[source_id] = (round_id, token)

    def _run_safely(self, source_id: int, round_id: int, token: CancelToken) -> None:
        try:
            self._run(source_id, round_id, token)
        except Exception:  # 线程内兜底，绝不让异常静默杀死线程
            log.exception("来源 %s 采集线程异常退出", source_id)
            try:
                conn = connect(self.config.db_path)
                current_round = token.round_id or round_id
                if token.is_set():
                    # 异常收尾也不得违背取消意图：继续完成取消清理
                    repository.finalize_cancel(conn, source_id, current_round)
                else:
                    repository.mark_failed(
                        conn, source_id, current_round,
                        "internal", "采集线程内部错误",
                    )
                conn.close()
            except Exception:
                log.exception("异常收尾处理也失败了")
        finally:
            with self._lock:
                cur = self._cancel_tokens.get(source_id)
                if cur is not None and cur[1] is token:
                    self._cancel_tokens.pop(source_id, None)

    def _run(self, source_id: int, round_id: int, token: CancelToken) -> None:
        src = next(s for s in self.config.sources if s["id"] == source_id)
        conn = connect(self.config.db_path)
        client = OaiClient(
            src["base_url"],
            self.config.oai_connect_timeout,
            self.config.oai_read_timeout,
        )
        log.info("来源 %s 采集开始（round=%s）", src["code"], round_id)
        cancelled = False
        try:
            while True:
                if token.is_set():
                    cancelled = True
                    break
                state = repository.get_state(conn, source_id)
                if state["status"] != "running" or state["round_id"] != round_id:
                    # 取消已被受理（状态被外部改变），退出抓取循环
                    cancelled = True
                    break
                oai_token = state["resumption_token"]
                # —— 网络等待在事务之外，不占数据库写事务 ——
                try:
                    page = client.fetch_page(oai_token, cancel=token)
                except HarvestCancelled:
                    cancelled = True
                    break
                except OaiNetworkError as exc:
                    if token.is_set():
                        cancelled = True
                        break
                    # 网络失败：进度与令牌都已落盘，保留 error 状态等待续采
                    repository.mark_failed(
                        conn, source_id, round_id, "network", str(exc)
                    )
                    log.warning("来源 %s 网络失败，可续采: %s", src["code"], exc)
                    return
                except OaiError as exc:
                    if token.is_set():
                        cancelled = True
                        break
                    if exc.code == "badResumptionToken":
                        if state["restarts"] >= self.config.max_token_restarts:
                            repository.mark_failed(
                                conn, source_id, round_id, "badResumptionToken",
                                f"重采次数超过上限 {self.config.max_token_restarts}",
                            )
                            return
                        new_round = repository.restart_from_beginning(
                            conn, source_id, str(exc), expected_round=round_id
                        )
                        if new_round is None:
                            cancelled = True  # 取消在重采推进前已受理
                            break
                        round_id = new_round
                        token.round_id = new_round
                        self._note_round(source_id, new_round, token)
                        log.warning(
                            "来源 %s 令牌失效，清空暂存从头重采（round=%s）",
                            src["code"], round_id,
                        )
                        continue
                    repository.mark_failed(
                        conn, source_id, round_id, exc.code, exc.message
                    )
                    return

                if token.is_set():
                    cancelled = True
                    break
                # —— 短事务：逐页持久化记录与下一页令牌（网络等待已在事务外完成）——
                next_seq = repository.persist_page(
                    conn, source_id, round_id, state["next_seq"], page.records
                )
                # advance_page 内部 BEGIN IMMEDIATE；取消受理后本轮落盘会被拒绝
                if not repository.advance_page(
                    conn, source_id, round_id, next_seq,
                    len(page.records), page.resumption_token,
                    page.complete_list_size,
                ):
                    cancelled = True
                    break

                if page.resumption_token is None:
                    try:
                        count = repository.complete_round(conn, source_id, round_id)
                    except repository.RoundCancelled:
                        cancelled = True  # 取消先受理，本轮发布被阻止
                        break
                    log.info(
                        "来源 %s 完整一轮结束，正式目录已替换（%s 条）",
                        src["code"], count,
                    )
                    return
        finally:
            if cancelled or token.is_set():
                # 采集线程退出抓取循环后才做取消收尾：
                # 清空本轮暂存与令牌，状态才置为 cancelled
                try:
                    repository.finalize_cancel(conn, source_id, round_id)
                except Exception:
                    log.exception("来源 %s 取消收尾失败", source_id)
            conn.close()

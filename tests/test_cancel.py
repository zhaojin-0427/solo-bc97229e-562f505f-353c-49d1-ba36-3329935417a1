"""取消采集回归测试。

覆盖：
- 慢正文场景：取消立即中断在途响应，不等慢正文发完、也不等读超时；
- 取消与整轮发布互斥（两个方向，含线程级交错）；
- 重复取消返回原结果；旧页面不能取消新轮；
- 取消中拒绝同源启动，结束后可从首页新采；
- 取消期间另一源采集与目录查询继续可用；
- 清理期间进程重启继续取消（不转成可续采错误）；
- 迟到响应与异常收尾不得修改新轮。
"""
import threading
import time
import unittest.mock

from conftest_helpers import BaseCase
from harvester import repository
from harvester.db import connect
from harvester.engine import HarvestManager
from harvester.oai import OaiRecord


def wait_state(db_path, manager, source_id, expected, timeout=20):
    """等待状态到达 expected 且后台线程已退出。"""
    deadline = time.time() + timeout
    conn = connect(db_path)
    try:
        while time.time() < deadline:
            row = dict(repository.get_state(conn, source_id))
            if row["status"] == expected and not manager.is_running(source_id):
                return row
            time.sleep(0.05)
        raise AssertionError(
            f"来源 {source_id} 未在 {timeout}s 内到达 {expected}，当前 {row}"
        )
    finally:
        conn.close()


class TestCancelRepository(BaseCase):
    """仓储层确定性测试（不依赖网络时序）。"""

    def test_publish_first_then_cancel_rejected(self):
        # 发布先提交 → 取消被拒绝，目录与状态不变
        conn = self.open_conn()
        _, r = repository.start_or_resume(conn, 1)
        repository.persist_page(
            conn, 1, r, 0, [OaiRecord("oai:a:p1", "题名", "10.1/X", False)]
        )
        self.assertTrue(repository.advance_page(conn, 1, r, 1, 1, None, 1))
        repository.complete_round(conn, 1, r)

        res = repository.request_cancel(conn, 1, r)
        self.assertFalse(res["ok"])
        self.assertEqual(res["result"], "rejected_published")
        self.assertEqual(repository.get_state(conn, 1)["status"], "success")
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM catalog_records WHERE source_id=1"
            ).fetchone()["n"],
            1,
        )
        conn.close()

    def test_cancel_first_then_publish_blocked(self):
        # 取消先受理 → 该轮发布被阻止，目录不被污染
        conn = self.open_conn()
        _, r = repository.start_or_resume(conn, 1)
        repository.persist_page(
            conn, 1, r, 0, [OaiRecord("oai:a:p1", "题名", "10.1/X", False)]
        )
        repository.advance_page(conn, 1, r, 1, 1, None, 1)
        res = repository.request_cancel(conn, 1, r)
        self.assertEqual(res["result"], "accepted")

        with self.assertRaises(repository.RoundCancelled):
            repository.complete_round(conn, 1, r)
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM catalog_records WHERE source_id=1"
            ).fetchone()["n"],
            0,
        )
        # 收尾：清暂存、清令牌、置 cancelled
        self.assertTrue(repository.finalize_cancel(conn, 1, r))
        st = repository.get_state(conn, 1)
        self.assertEqual(st["status"], "cancelled")
        self.assertIsNone(st["resumption_token"])
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM staging_records WHERE source_id=1"
            ).fetchone()["n"],
            0,
        )
        conn.close()

    def test_repeated_cancel_returns_original_result(self):
        conn = self.open_conn()
        repository.start_or_resume(conn, 1)
        r = repository.get_state(conn, 1)["round_id"]
        first = repository.request_cancel(conn, 1, r)
        second = repository.request_cancel(conn, 1, r)
        self.assertEqual(first["result"], "accepted")
        self.assertTrue(first["ok"])
        # 重复取消：同一轮次返回原受理结果，不产生错误也不开启新取消
        self.assertEqual(second["result"], "already_cancelling")
        self.assertTrue(second["ok"])
        repository.finalize_cancel(conn, 1, r)
        third = repository.request_cancel(conn, 1, r)
        self.assertEqual(third["result"], "already_cancelled")
        self.assertTrue(third["ok"])
        conn.close()

    def test_old_page_cannot_cancel_new_round(self):
        conn = self.open_conn()
        repository.start_or_resume(conn, 1)
        r1 = repository.get_state(conn, 1)["round_id"]
        repository.request_cancel(conn, 1, r1)
        repository.finalize_cancel(conn, 1, r1)
        # 新一轮开始（旧页面仍持有 r1）
        repository.start_or_resume(conn, 1)
        r2 = repository.get_state(conn, 1)["round_id"]
        self.assertEqual(r2, r1 + 1)

        res = repository.request_cancel(conn, 1, r1)
        self.assertFalse(res["ok"])
        self.assertEqual(res["result"], "rejected_stale_round")
        st = repository.get_state(conn, 1)
        self.assertEqual(st["status"], "running")  # 新轮不受影响
        self.assertEqual(st["round_id"], r2)
        conn.close()

    def test_start_rejected_while_cancelling(self):
        conn = self.open_conn()
        repository.start_or_resume(conn, 1)
        r = repository.get_state(conn, 1)["round_id"]
        repository.request_cancel(conn, 1, r)
        # 取消收尾期间拒绝同源启动
        with self.assertRaises(repository.CancelInProgress):
            repository.start_or_resume(conn, 1)
        # 收尾结束后可以从首页新采
        repository.finalize_cancel(conn, 1, r)
        action, r2 = repository.start_or_resume(conn, 1)
        self.assertEqual(action, "started")
        self.assertEqual(r2, r + 1)
        conn.close()

    def test_restart_continues_cancel_not_resumable_error(self):
        # 构造「清理期间进程崩溃」：cancelling 已持久化，暂存与令牌还在
        conn = self.open_conn()
        repository.start_or_resume(conn, 1)
        r = repository.get_state(conn, 1)["round_id"]
        repository.persist_page(
            conn, 1, r, 0,
            [OaiRecord("oai:a:p1", "t1", "10.1/A", False),
             OaiRecord("oai:a:p2", "t2", "10.1/B", False)],
        )
        repository.advance_page(conn, 1, r, 2, 2, "tok-1", 10)
        res = repository.request_cancel(conn, 1, r)
        self.assertEqual(res["result"], "accepted")
        conn.close()

        # 模拟进程重启：新 manager + 启动恢复
        manager2 = HarvestManager(self.config)
        conn2 = connect(self.db_path)
        recovered = repository.recover_orphan_runs(conn2)
        st = repository.get_state(conn2, 1)
        # 继续完成取消：不转成可续采的 error/interrupted
        self.assertNotIn(1, recovered)
        self.assertEqual(st["status"], "cancelled")
        self.assertIsNone(st["resumption_token"])
        self.assertIsNone(st["error_code"])
        self.assertEqual(
            conn2.execute(
                "SELECT COUNT(*) n FROM staging_records WHERE source_id=1"
            ).fetchone()["n"],
            0,
        )
        conn2.close()

        # 重启后从首页新采（不是续采）
        action = manager2.start(1)
        self.assertEqual(action, "started")
        final = wait_state(self.db_path, manager2, 1, "success")
        self.assertEqual(final["round_id"], r + 1)
        conn3 = self.open_conn()
        self.assertEqual(
            repository.catalog_stats(conn3)["per_source"][0]["total"], 30
        )
        conn3.close()

    def test_late_writes_do_not_touch_new_round(self):
        conn = self.open_conn()
        repository.start_or_resume(conn, 1)
        r1 = repository.get_state(conn, 1)["round_id"]
        repository.persist_page(
            conn, 1, r1, 0, [OaiRecord("oai:a:old", "旧", "10.1/OLD", False)]
        )
        repository.advance_page(conn, 1, r1, 1, 1, "tok-1", 5)
        repository.request_cancel(conn, 1, r1)
        repository.finalize_cancel(conn, 1, r1)

        # 新一轮进行中
        repository.start_or_resume(conn, 1)
        r2 = repository.get_state(conn, 1)["round_id"]
        repository.persist_page(
            conn, 1, r2, 0, [OaiRecord("oai:a:new", "新", "10.1/NEW", False)]
        )
        repository.advance_page(conn, 1, r2, 1, 1, "tok-2", 5)
        before = dict(repository.get_state(conn, 1))

        # 旧轮的迟到写入/异常收尾：一律不得生效
        self.assertFalse(
            repository.advance_page(conn, 1, r1, 99, 99, "tok-late", 5)
        )
        repository.mark_failed(conn, 1, r1, "network", "late failure")
        with self.assertRaises(repository.RoundCancelled):
            repository.complete_round(conn, 1, r1)
        self.assertIsNone(
            repository.restart_from_beginning(conn, 1, "late", expected_round=r1)
        )
        # 旧轮暂存不得复活
        repository.persist_page(
            conn, 1, r1, 0, [OaiRecord("oai:a:ghost", "鬼", "10.1/G", False)]
        )

        after = dict(repository.get_state(conn, 1))
        self.assertEqual(after["status"], "running")
        self.assertEqual(after["round_id"], r2)
        self.assertEqual(after["resumption_token"], "tok-2")
        self.assertEqual(after["records_fetched"], before["records_fetched"])
        self.assertEqual(after["pages_done"], before["pages_done"])
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM staging_records "
                "WHERE source_id=1 AND round_id=?",
                (r1,),
            ).fetchone()["n"],
            0,
        )
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM staging_records "
                "WHERE source_id=1 AND round_id=?",
                (r2,),
            ).fetchone()["n"],
            1,
        )
        conn.close()


class TestCancelSlowBody(BaseCase):
    """慢正文机构库上的端到端取消测试。"""

    def setUp(self):
        super().setUp()
        # 持续慢正文：每 128 字节睡 0.5s，一页约 3KB → 单页约 12s 才能发完
        self.state_a.slow_body_sticky = True
        self.state_a.slow_chunk_seconds = 0.5
        self.state_a.slow_chunk_bytes = 128

    def _round_of(self, source_id: int) -> int:
        conn = self.open_conn()
        try:
            return repository.get_state(conn, source_id)["round_id"]
        finally:
            conn.close()

    def test_cancel_interrupts_slow_response(self):
        # 慢正文接收中取消：立即中断，不等正文发完（约12s/页）或读超时（15s）
        self.assertEqual(self.manager.start(1), "started")
        time.sleep(1.0)  # 确保 worker 已进入慢正文接收
        r1 = self._round_of(1)
        t0 = time.time()
        res = self.manager.cancel(1, r1)
        self.assertEqual(res["result"], "accepted")
        row = self.wait_status(1, "cancelled", timeout=10)
        elapsed = time.time() - t0
        self.assertLess(elapsed, 8, "取消必须立即中断慢正文，不等正文结束或读超时")
        # 采集已退出、暂存与令牌已清除
        self.assertIsNone(row["resumption_token"])
        self.assertFalse(self.manager.is_running(1))
        conn = self.open_conn()
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM staging_records WHERE source_id=1"
            ).fetchone()["n"],
            0,
        )
        # 本轮未发布：正式目录为空
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM catalog_records WHERE source_id=1"
            ).fetchone()["n"],
            0,
        )
        conn.close()

    def test_cancel_immediately_after_start(self):
        # 首页响应还未到就取消
        self.manager.start(1)
        r1 = self._round_of(1)
        res = self.manager.cancel(1, r1)
        self.assertTrue(res["ok"])
        self.assertEqual(res["result"], "accepted")
        row = self.wait_status(1, "cancelled", timeout=10)
        self.assertIsNone(row["resumption_token"])
        conn = self.open_conn()
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM staging_records WHERE source_id=1"
            ).fetchone()["n"],
            0,
        )
        conn.close()

    def test_repeated_cancel_engine(self):
        self.manager.start(1)
        time.sleep(0.8)
        r1 = self._round_of(1)
        first = self.manager.cancel(1, r1)
        second = self.manager.cancel(1, r1)
        self.assertEqual(first["result"], "accepted")
        # 重复取消返回原结果：清理快时可能已 cancelled，慢时仍在 cancelling
        self.assertIn(second["result"], ("already_cancelling", "already_cancelled"))
        self.assertTrue(second["ok"])
        self.wait_status(1, "cancelled", timeout=10)
        third = self.manager.cancel(1, r1)
        self.assertEqual(third["result"], "already_cancelled")
        self.assertTrue(third["ok"])

    def test_new_round_after_cancel_from_first_page(self):
        self.manager.start(1)
        time.sleep(0.8)
        r1 = self._round_of(1)
        self.manager.cancel(1, r1)
        row = self.wait_status(1, "cancelled", timeout=10)
        self.assertEqual(row["round_id"], r1)

        # 结束后可从首页新采（不是续采）
        self.state_a.slow_body_sticky = False
        action = self.manager.start(1)
        self.assertEqual(action, "started")
        final = self.wait_status(1, "success")
        self.assertEqual(final["round_id"], r1 + 1)
        conn = self.open_conn()
        self.assertEqual(
            repository.catalog_stats(conn)["per_source"][0]["total"], 30
        )
        conn.close()

    def test_stale_cancel_does_not_interrupt_new_round(self):
        self.manager.start(1)
        time.sleep(0.8)
        r1 = self._round_of(1)
        self.manager.cancel(1, r1)
        self.wait_status(1, "cancelled", timeout=10)

        # 新一轮进行中，旧页面的取消请求被拒绝且不影响新轮
        self.state_a.slow_body_sticky = False
        self.assertEqual(self.manager.start(1), "started")
        r2 = self._round_of(1)
        res = self.manager.cancel(1, r1)
        self.assertFalse(res["ok"])
        self.assertEqual(res["result"], "rejected_stale_round")
        final = self.wait_status(1, "success")
        self.assertEqual(final["round_id"], r2)

    def test_other_source_and_catalog_available_during_cancel(self):
        self.manager.start(1)
        time.sleep(0.8)
        r1 = self._round_of(1)
        self.manager.cancel(1, r1)

        # 另一源采集照常
        self.assertEqual(self.manager.start(2), "started")
        self.wait_status(2, "success")
        self.wait_status(1, "cancelled", timeout=10)

        # 目录查询继续可用：B 完整在架，A 无半截数据
        conn = self.open_conn()
        stats = repository.catalog_stats(conn)
        b = next(s for s in stats["per_source"] if s["code"] == "B")
        a = next(s for s in stats["per_source"] if s["code"] == "A")
        self.assertEqual(b["total"], 34)
        self.assertEqual(a["total"], 0)
        rows, total = repository.search_catalog(conn, None, 10, 0)
        self.assertGreater(total, 0)
        conn.close()

    def test_start_during_cancelling_then_recovers(self):
        self.manager.start(1)
        time.sleep(0.8)
        r1 = self._round_of(1)
        self.manager.cancel(1, r1)
        self.state_a.slow_body_sticky = False  # 让后续新轮快速完成

        # 取消收尾期间同源启动被拒绝；收尾结束后可直接新采
        actions = []
        deadline = time.time() + 15
        while time.time() < deadline:
            action = self.manager.start(1)
            actions.append(action)
            if action == "started":
                break
            time.sleep(0.05)
        self.assertIn("started", actions)
        self.assertNotIn("reused", actions)   # 取消中绝不可复用旧任务
        self.assertNotIn("resumed", actions)  # 取消后只能新采，不能续采
        final = self.wait_status(1, "success")
        self.assertEqual(final["round_id"], r1 + 1)


class TestCancelVsPublishEngine(BaseCase):
    """取消与整轮发布的交错（引擎级）。"""

    def test_cancel_after_publish_rejected(self):
        self.manager.start(1)
        row = self.wait_status(1, "success")
        res = self.manager.cancel(1, row["round_id"])
        self.assertFalse(res["ok"])
        self.assertEqual(res["result"], "rejected_published")
        conn = self.open_conn()
        self.assertEqual(
            repository.catalog_stats(conn)["per_source"][0]["total"], 30
        )
        self.assertEqual(repository.get_state(conn, 1)["status"], "success")
        conn.close()

    def test_cancel_wins_over_concurrent_publish(self):
        # 让发布在事务前等待：取消先受理，随后放行的发布必须被阻止
        entered = threading.Event()
        release = threading.Event()
        original = repository.complete_round

        def gated_complete(conn, source_id, round_id):
            entered.set()
            release.wait(10)
            return original(conn, source_id, round_id)

        with unittest.mock.patch.object(
            repository, "complete_round", gated_complete
        ):
            self.manager.start(1)
            self.assertTrue(entered.wait(10), "采集线程应到达发布点")
            conn = self.open_conn()
            r1 = repository.get_state(conn, 1)["round_id"]
            conn.close()
            res = self.manager.cancel(1, r1)
            self.assertEqual(res["result"], "accepted")
            release.set()
            row = self.wait_status(1, "cancelled", timeout=10)
            self.assertEqual(row["round_id"], r1)

        # 发布被阻止：正式目录为空，暂存已清
        conn = self.open_conn()
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM catalog_records WHERE source_id=1"
            ).fetchone()["n"],
            0,
        )
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) n FROM staging_records WHERE source_id=1"
            ).fetchone()["n"],
            0,
        )
        conn.close()


if __name__ == "__main__":
    import unittest

    unittest.main()

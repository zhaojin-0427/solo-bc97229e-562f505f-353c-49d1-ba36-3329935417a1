"""数据访问层：采集状态机、暂存持久化、整轮交换、合并目录查询。"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Iterable

from .doi_utils import normalize_doi
from .oai import OaiRecord


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class AlreadyRunning(Exception):
    """该来源已有采集任务在运行（同源重复启动应复用而非新建）。"""


class CancelInProgress(Exception):
    """该来源正在取消收尾（cancelling），暂不能启动新采集。"""


class RoundCancelled(Exception):
    """本轮已被取消受理：禁止发布/落盘/标记，采集线程应转入取消收尾。"""


# ---------------- 来源与状态 ----------------

def list_sources(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM sources ORDER BY priority, id"
    ).fetchall()


def get_state(conn: sqlite3.Connection, source_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM harvest_state WHERE source_id=?", (source_id,)
    ).fetchone()
    if row is None:
        raise KeyError(f"未知来源: {source_id}")
    return row


def all_states(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT s.*, h.* FROM sources s "
        "JOIN harvest_state h ON h.source_id=s.id "
        "ORDER BY s.priority, s.id"
    ).fetchall()


def recover_orphan_runs(conn: sqlite3.Connection) -> list[int]:
    """进程重启恢复。

    - ``cancelling``：取消意图已持久化，继续完成取消清理（删暂存、清令牌、
      置 cancelled），绝不转成可续采的 error；
    - ``running``：上次崩溃残留的采集，标记 error/interrupted 以便续采。
    """
    cancelling = conn.execute(
        "SELECT source_id, round_id FROM harvest_state WHERE status='cancelling'"
    ).fetchall()
    for row in cancelling:
        finalize_cancel(conn, row["source_id"], row["round_id"])
    now = utcnow()
    rows = conn.execute(
        "SELECT source_id FROM harvest_state WHERE status='running'"
    ).fetchall()
    if not rows:
        return []
    conn.execute("BEGIN IMMEDIATE")
    try:
        for row in rows:
            conn.execute(
                "UPDATE harvest_state SET status='error', "
                "error_code='interrupted', error_message='进程重启，可续采', "
                "finished_at=?, updated_at=? WHERE source_id=?",
                (now, now, row["source_id"]),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return [r["source_id"] for r in rows]


# ---------------- 采集轮次生命周期 ----------------

def start_or_resume(conn: sqlite3.Connection, source_id: int) -> tuple[str, int]:
    """启动或续采。返回 (动作, round_id)，动作取值 started / resumed。

    - error 且暂存仍在 -> 续采（沿用 round_id 与 resumptionToken）；
    - idle / success / cancelled -> 开启新一轮，清空该源旧暂存（正式目录不动）；
    - running -> AlreadyRunning；cancelling -> CancelInProgress（取消中拒绝启动）。
    """
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_state(conn, source_id)
        if row["status"] == "running":
            # 未开启自己的事务，无需 rollback
            raise AlreadyRunning(source_id)
        if row["status"] == "cancelling":
            raise CancelInProgress(source_id)

        has_staging = conn.execute(
            "SELECT 1 FROM staging_records WHERE source_id=? AND round_id=? LIMIT 1",
            (source_id, row["round_id"]),
        ).fetchone()
        if row["status"] == "error" and has_staging:
            conn.execute(
                "UPDATE harvest_state SET status='running', error_code=NULL, "
                "error_message=NULL, finished_at=NULL, updated_at=? "
                "WHERE source_id=?",
                (now, source_id),
            )
            round_id = row["round_id"]
            action = "resumed"
        else:
            round_id = row["round_id"] + 1
            conn.execute(
                "DELETE FROM staging_records WHERE source_id=?", (source_id,)
            )
            conn.execute(
                "UPDATE harvest_state SET status='running', round_id=?, "
                "resumption_token=NULL, next_seq=0, pages_done=0, "
                "records_fetched=0, total_expected=NULL, error_code=NULL, "
                "error_message=NULL, restarts=0, started_at=?, updated_at=?, "
                "finished_at=NULL WHERE source_id=?",
                (round_id, now, now, source_id),
            )
            action = "started"
        conn.commit()
        return action, round_id
    except Exception:
        conn.rollback()
        raise


def restart_from_beginning(
    conn: sqlite3.Connection, source_id: int, reason: str,
    expected_round: int | None = None,
) -> int | None:
    """badResumptionToken 处理：清空本轮暂存并从头重采，正式目录保持不变。

    取消已受理（状态不再 running）或轮次已不匹配时不推进，返回 None。"""
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_state(conn, source_id)
        if row["status"] != "running":
            conn.rollback()
            return None
        if expected_round is not None and row["round_id"] != expected_round:
            conn.rollback()
            return None
        old_round = row["round_id"]
        new_round = old_round + 1
        conn.execute(
            "DELETE FROM staging_records WHERE source_id=? AND round_id=?",
            (source_id, old_round),
        )
        conn.execute(
            "UPDATE harvest_state SET status='running', round_id=?, "
            "resumption_token=NULL, next_seq=0, pages_done=0, records_fetched=0, "
            "total_expected=NULL, restarts=restarts+1, error_code=NULL, "
            "error_message=NULL, updated_at=? WHERE source_id=?",
            (new_round, now, source_id),
        )
        conn.commit()
        return new_round
    except Exception:
        conn.rollback()
        raise


def persist_page(
    conn: sqlite3.Connection,
    source_id: int,
    round_id: int,
    start_seq: int,
    records: Iterable[OaiRecord],
) -> int:
    """逐页持久化一页记录，返回下一条序号（调用方与 advance_page 组成短事务）。

    仅当本轮仍在运行时写入；取消受理后迟到的落盘直接丢弃（返回原序号），
    不得在已取消/已推进的轮次上复活暂存数据。
    """
    row = conn.execute(
        "SELECT status, round_id FROM harvest_state WHERE source_id=?",
        (source_id,),
    ).fetchone()
    if row is None or row["status"] != "running" or row["round_id"] != round_id:
        return start_seq
    seq = start_seq
    rows = []
    for rec in records:
        doi_key = normalize_doi(rec.doi_raw)
        rows.append(
            (
                source_id,
                round_id,
                rec.oai_id,
                rec.title,
                rec.doi_raw,
                doi_key,
                1 if rec.deleted else 0,
                seq,
            )
        )
        seq += 1
    conn.executemany(
        """INSERT OR REPLACE INTO staging_records
           (source_id, round_id, oai_identifier, title, doi_raw, doi_key,
            is_deleted, seq)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
    return seq


def advance_page(
    conn: sqlite3.Connection,
    source_id: int,
    round_id: int,
    next_seq: int,
    page_records: int,
    token: str | None,
    total_expected: int | None,
) -> bool:
    """保存一页进度与 resumptionToken（单个短事务，立即原子落盘）。

    仅当本轮仍在运行（status='running' 且轮次一致）时才生效并返回 True；
    取消受理后迟到的推进会被拒绝并返回 False，由采集线程转入取消收尾。
    """
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "UPDATE harvest_state SET resumption_token=?, next_seq=?, "
            "pages_done=pages_done+1, records_fetched=records_fetched+?, "
            "total_expected=COALESCE(?, total_expected), updated_at=? "
            "WHERE source_id=? AND round_id=? AND status='running'",
            (token, next_seq, page_records, total_expected, now, source_id, round_id),
        )
        conn.commit()
        return cur.rowcount > 0
    except Exception:
        conn.rollback()
        raise


def mark_failed(
    conn: sqlite3.Connection, source_id: int, round_id: int, code: str, message: str
) -> None:
    """标记本轮失败（可续采）。

    仅在本轮仍在运行时生效：取消受理后、或新一轮已开始后，迟到/异常收尾
    的失败标记一律丢弃，不得修改新轮状态。
    """
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE harvest_state SET status='error', error_code=?, error_message=?, "
            "finished_at=?, updated_at=? "
            "WHERE source_id=? AND round_id=? AND status='running'",
            (code, message, now, now, source_id, round_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def complete_round(
    conn: sqlite3.Connection, source_id: int, round_id: int
) -> int:
    """完整一轮成功后，在单个事务内替换该来源的正式目录。

    只删除/重写本来源行，另一来源的目录与并发更新原样保留；
    事务提交前查询仍读到旧目录（只见完整状态）。

    OAI 撤稿 header 不携带 metadata，撤稿记录没有 DOI/题名；
    交换前按 OAI 标识从该来源旧目录回填，保证「展示全部出处」时
    撤稿记录仍挂在原 DOI 下。

    取消与整轮发布互斥：本函数与 request_cancel 都是 BEGIN IMMEDIATE
    写事务，SQLite 写锁保证只有一方先提交；若取消已受理（状态不再是
    本轮 running），发布被拒绝并抛 :class:`RoundCancelled`。
    """
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_state(conn, source_id)
        if row["status"] != "running" or row["round_id"] != round_id:
            raise RoundCancelled(
                f"第 {round_id} 轮已被取消或不再运行，禁止发布"
            )
        conn.execute(
            """UPDATE staging_records
               SET doi_raw = old.doi_raw,
                   doi_key = old.doi_key,
                   title   = COALESCE(NULLIF(staging_records.title, ''), old.title)
               FROM catalog_records AS old
               WHERE staging_records.source_id = old.source_id
                 AND staging_records.oai_identifier = old.oai_identifier
                 AND staging_records.source_id = ?
                 AND staging_records.round_id = ?
                 AND staging_records.is_deleted = 1""",
            (source_id, round_id),
        )
        staged = conn.execute(
            "SELECT COUNT(*) AS n FROM staging_records "
            "WHERE source_id=? AND round_id=?",
            (source_id, round_id),
        ).fetchone()["n"]
        conn.execute(
            "DELETE FROM catalog_records WHERE source_id=?", (source_id,)
        )
        conn.execute(
            """INSERT INTO catalog_records
               (source_id, oai_identifier, title, doi_raw, doi_key,
                is_deleted, harvested_round, harvested_at)
               SELECT source_id, oai_identifier, title, doi_raw, doi_key,
                      is_deleted, round_id, ?
               FROM staging_records
               WHERE source_id=? AND round_id=?""",
            (now, source_id, round_id),
        )
        conn.execute(
            "UPDATE harvest_state SET status='success', resumption_token=NULL, "
            "finished_at=?, last_success_at=?, updated_at=? "
            "WHERE source_id=? AND round_id=?",
            (now, now, now, source_id, round_id),
        )
        conn.commit()
        return staged
    except Exception:
        conn.rollback()
        raise


# ---------------- 取消本轮采集 ----------------

def request_cancel(
    conn: sqlite3.Connection, source_id: int, round_id: int
) -> dict:
    """受理取消意图并持久化（status 置 cancelling）。

    取消与整轮发布只能一方成功：两者都是 BEGIN IMMEDIATE 写事务，
    若 complete_round 已先提交（status='success'），此处拒绝取消。

    返回 dict：ok（受理或幂等成功）、result、message、status。
    result 取值：
    - accepted              本次受理成功；
    - already_cancelling    重复取消：该轮已在取消中，返回原结果；
    - already_cancelled     重复取消：该轮已取消，返回原结果；
    - rejected_published    该轮发布已提交，取消被拒绝；
    - rejected_stale_round  轮次不匹配（旧页面不能取消新轮）；
    - rejected_not_running  该轮未在采集，无需取消。
    """
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_state(conn, source_id)
        status = row["status"]
        current_round = row["round_id"]
        if current_round != round_id:
            result = {
                "ok": False,
                "result": "rejected_stale_round",
                "message": f"轮次不匹配：当前为第 {current_round} 轮，"
                f"旧页面不能取消新轮（请求的是第 {round_id} 轮）",
            }
        elif status == "running":
            conn.execute(
                "UPDATE harvest_state SET status='cancelling', updated_at=? "
                "WHERE source_id=? AND round_id=? AND status='running'",
                (now, source_id, round_id),
            )
            result = {
                "ok": True,
                "result": "accepted",
                "message": "取消已受理：将中断在途响应，清理本轮暂存与令牌",
            }
            status = "cancelling"
        elif status == "cancelling":
            result = {
                "ok": True,
                "result": "already_cancelling",
                "message": "该轮取消已受理，正在清理（重复取消返回原结果）",
            }
        elif status == "cancelled":
            result = {
                "ok": True,
                "result": "already_cancelled",
                "message": "该轮已取消（重复取消返回原结果）",
            }
        elif status == "success":
            result = {
                "ok": False,
                "result": "rejected_published",
                "message": "该轮完整发布已提交，取消被拒绝",
            }
        else:  # idle / error
            result = {
                "ok": False,
                "result": "rejected_not_running",
                "message": f"该轮未在采集（状态 {status}），无需取消",
            }
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    result.update(
        {"source_id": source_id, "round_id": round_id, "status": status}
    )
    return result


def finalize_cancel(
    conn: sqlite3.Connection, source_id: int, round_id: int
) -> bool:
    """取消收尾：删除本轮暂存、清除令牌，状态置 cancelled。幂等。

    只在采集线程真正退出抓取循环后（或进程重启由恢复逻辑）执行，
    因此「已取消」必然意味着：采集已停、暂存已清、令牌已清。
    """
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = get_state(conn, source_id)
        if row["status"] != "cancelling" or row["round_id"] != round_id:
            conn.rollback()
            return False
        conn.execute(
            "DELETE FROM staging_records WHERE source_id=? AND round_id=?",
            (source_id, round_id),
        )
        conn.execute(
            "UPDATE harvest_state SET status='cancelled', resumption_token=NULL, "
            "error_code=NULL, error_message=NULL, finished_at=?, updated_at=? "
            "WHERE source_id=? AND round_id=?",
            (now, now, source_id, round_id),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


# ---------------- 目录查询（只读，永远指向完整快照） ----------------

def catalog_stats(conn: sqlite3.Connection) -> dict:
    per_source = []
    for src in list_sources(conn):
        row = conn.execute(
            "SELECT COUNT(*) AS total, "
            "SUM(CASE WHEN is_deleted=0 THEN 1 ELSE 0 END) AS active, "
            "SUM(CASE WHEN is_deleted=1 THEN 1 ELSE 0 END) AS deleted "
            "FROM catalog_records WHERE source_id=?",
            (src["id"],),
        ).fetchone()
        state = get_state(conn, src["id"])
        per_source.append(
            {
                "code": src["code"],
                "name": src["name"],
                "priority": src["priority"],
                "total": row["total"] or 0,
                "active": row["active"] or 0,
                "deleted": row["deleted"] or 0,
                "last_success_at": state["last_success_at"],
            }
        )
    merged = conn.execute("SELECT COUNT(*) AS n FROM merged_catalog").fetchone()["n"]
    shared = conn.execute(
        "SELECT COUNT(*) AS n FROM merged_catalog WHERE source_count > 1"
    ).fetchone()["n"]
    no_doi = conn.execute(
        "SELECT COUNT(*) AS n FROM catalog_records WHERE doi_key IS NULL AND is_deleted=0"
    ).fetchone()["n"]
    return {
        "per_source": per_source,
        "merged_doi_count": merged,
        "shared_doi_count": shared,
        "no_doi_count": no_doi,
    }


def search_catalog(
    conn: sqlite3.Connection, query: str | None, limit: int, offset: int
) -> tuple[list[sqlite3.Row], int]:
    if query:
        like = f"%{query.strip().lower()}%"
        where = "WHERE doi_key LIKE ? OR LOWER(preferred_title) LIKE ?"
        params: tuple = (like, like)
    else:
        where = ""
        params = ()
    total = conn.execute(
        f"SELECT COUNT(*) AS n FROM merged_catalog {where}", params
    ).fetchone()["n"]
    rows = conn.execute(
        f"SELECT * FROM merged_catalog {where} "
        f"ORDER BY doi_key LIMIT ? OFFSET ?",
        params + (limit, offset),
    ).fetchall()
    return rows, total


def get_merged_doi(conn: sqlite3.Connection, doi_key: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM merged_catalog WHERE doi_key=?", (doi_key.lower(),)
    ).fetchone()


def provenance(conn: sqlite3.Connection, doi_key: str) -> list[sqlite3.Row]:
    """展示该 DOI 的全部出处（含已被本来源撤下的记录）。"""
    return conn.execute(
        """SELECT c.oai_identifier, c.title, c.doi_raw, c.is_deleted,
                  c.harvested_round, c.harvested_at,
                  s.id AS source_id, s.code, s.name, s.priority
           FROM catalog_records c JOIN sources s ON s.id = c.source_id
           WHERE c.doi_key=? ORDER BY s.priority, s.id""",
        (doi_key.lower(),),
    ).fetchall()


def list_without_doi(conn: sqlite3.Connection, limit: int = 100) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT c.oai_identifier, c.title, c.harvested_at,
                  s.code, s.name, s.priority
           FROM catalog_records c JOIN sources s ON s.id = c.source_id
           WHERE c.doi_key IS NULL AND c.is_deleted=0
           ORDER BY s.priority, c.oai_identifier LIMIT ?""",
        (limit,),
    ).fetchall()

"""本地 OAI-PMH 2.0 模拟服务器（仅依赖标准库）。

支持：
- GET /oai?verb=ListRecords&metadataPrefix=oai_dc 首页；
- GET /oai?verb=ListRecords&resumptionToken=<token> 翻页，令牌可被控制台作废；
- 故障注入：expire_token（一次性 badResumptionToken）、stream_break（一次性断流 500）、
  slow_body（慢正文：持续滴流发送响应体，用于演示/测试取消中断）、
  delete/restore（撤稿与恢复）、reset（数据集还原）；
- GET /panel 故障注入控制台页面。

状态保存在内存，多线程访问加锁；令牌编码 epoch+cursor，撤稿不会使已发令牌失效，
但「令牌过期」开关会让下一次翻页返回 badResumptionToken。
"""
from __future__ import annotations

import argparse
import html
import json
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .dataset import build_dataset

OAI_NS = "http://www.openarchives.org/OAI/2.0/"


class MockState:
    def __init__(
        self,
        code: str,
        seed: int,
        count: int,
        registrant: str,
        page_size: int,
        overlap: int = 0,
        overlap_registrant: str | None = None,
        slow_chunk_seconds: float = 0.3,
        slow_chunk_bytes: int = 256,
    ):
        self.code = code
        self.seed = seed
        self.count = count
        self.registrant = registrant
        self.page_size = page_size
        self.overlap = overlap
        self.overlap_registrant = overlap_registrant
        self.lock = threading.Lock()
        self.epoch = 1
        self.records: list[dict] = []
        self.token_expired = False     # 一次性：下一次翻页 badToken，随后自动复位
        self.token_expired_sticky = False  # 持续模式：所有翻页一律 badToken
        self.break_once = False        # 一次性：下一次翻页断流
        self.slow_body_once = False    # 一次性：下一次 ListRecords 响应慢正文
        self.slow_body_sticky = False  # 持续模式：所有 ListRecords 响应都慢正文
        self.slow_chunk_seconds = slow_chunk_seconds  # 每个正文分块之间的间隔
        self.slow_chunk_bytes = slow_chunk_bytes      # 每个正文分块的字节数
        self.event_log: list[str] = []
        self.reset()

    def reset(self) -> None:
        with self.lock:
            self.records = build_dataset(
                self.code,
                self.seed,
                self.count,
                self.registrant,
                overlap=self.overlap,
                overlap_registrant=self.overlap_registrant,
            )
            # 数据集还原才使旧令牌作废；撤稿/恢复不经过这里
            self.epoch += 1
            self.token_expired = False
            self.token_expired_sticky = False
            self.break_once = False
            self.slow_body_once = False
            self.slow_body_sticky = False
            self._log("数据集已重置")

    def _log(self, msg: str) -> None:
        from datetime import datetime

        self.event_log.append(f"{datetime.now():%H:%M:%S} {msg}")
        self.event_log = self.event_log[-30:]

    def make_token(self, cursor: int) -> str:
        return f"v{self.epoch}-{cursor:06d}"

    def parse_token(self, token: str) -> tuple[int, int] | None:
        try:
            head, raw = token.split("-", 1)
            return int(head[1:]), int(raw)
        except (ValueError, IndexError):
            return None


def _oai_envelope(body: str) -> bytes:
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<OAI-PMH xmlns="{OAI_NS}">\n'
        "<responseDate>2026-11-13T00:00:00Z</responseDate>\n"
        f"{body}\n</OAI-PMH>\n"
    )
    return xml.encode("utf-8")


def _error_xml(verb: str, code: str, message: str) -> bytes:
    body = (
        f'<request>{html.escape(verb)}</request>'
        f'<error code="{html.escape(code)}">{html.escape(message)}</error>'
    )
    return _oai_envelope(body)


def _record_xml(rec: dict) -> str:
    if rec["deleted"]:
        return (
            "<record><header status=\"deleted\">"
            f"<identifier>{html.escape(rec['oai_id'])}</identifier>"
            "<datestamp>2026-11-10</datestamp></header></record>"
        )
    ids = ""
    if rec["doi"]:
        ids = f'<dc:identifier>{html.escape(rec["doi"])}</dc:identifier>'
    return (
        "<record><header>"
        f"<identifier>{html.escape(rec['oai_id'])}</identifier>"
        "<datestamp>2026-11-10</datestamp></header><metadata>"
        '<oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/">'
        f"<dc:title>{html.escape(rec['title'])}</dc:title>"
        f"{ids}</oai_dc:dc></metadata></record>"
    )


PANEL_HTML = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>来源 {code} 故障注入控制台</title><style>
body{{font-family:"PingFang SC","Microsoft YaHei",sans-serif;margin:0;background:#f5f7fa;color:#1c2430}}
header{{background:#7a2e8f;color:#fff;padding:16px 28px}} main{{max-width:900px;margin:24px auto;padding:0 20px}}
.card{{background:#fff;border:1px solid #dde3ea;border-radius:10px;padding:18px 22px;margin-bottom:18px}}
table{{width:100%;border-collapse:collapse;font-size:13.5px}}td,th{{padding:7px 9px;border-bottom:1px solid #eef1f5;text-align:left}}
button{{background:#7a2e8f;color:#fff;border:none;border-radius:7px;padding:9px 18px;margin:4px 6px 4px 0;cursor:pointer;font-size:14px}}
.warn{{background:#b25e09}} .danger{{background:#b42318}} .ok{{background:#177245}}
code{{background:#f0f3f7;padding:1px 6px;border-radius:4px}} pre{{background:#1c2430;color:#d6e2f0;padding:12px;border-radius:8px;font-size:12px;overflow:auto}}
.muted{{color:#7a8794;font-size:12.5px}}
</style></head><body><header><h2>🛠 OAI 来源 {code} 故障注入控制台</h2></header><main>
<div class="card"><h3>采集故障</h3>
<form method="post" action="/admin/expire-token"><button class="warn">让下一次翻页令牌过期（一次性 badResumptionToken）</button></form>
<form method="post" action="/admin/expire-token-sticky"><button class="warn">让令牌持续过期（反复重采直到上限，旧目录不变）</button></form>
<form method="post" action="/admin/stream-break"><button class="danger">让下一次翻页断流（HTTP 500，可续采）</button></form>
<form method="post" action="/admin/slow-body"><button class="warn">让下一次响应慢正文（持续滴流，演示取消中断）</button></form>
<form method="post" action="/admin/slow-body-sticky"><button class="warn">开启持续慢正文模式（每页滴流）</button></form>
<form method="post" action="/admin/slow-body-off"><button class="ok">关闭慢正文模式</button></form>
<form method="post" action="/admin/reset"><button class="ok">恢复数据集到初始状态</button></form>
<p class="muted">一次性故障触发后自动复位；采集器对断流支持断点续采，对令牌失效会清空本轮暂存从头重采；
慢正文模式下可在采集控制台点「取消本轮采集」，采集端会立即中断在途响应，不等正文发完或读超时。</p></div>
<div class="card"><h3>撤稿控制</h3>
<form method="post" action="/admin/delete"><label>OAI 标识 <input name="oai_id" size="34" value="{first_id}" required></label>
<button class="danger" type="submit">撤下该记录</button></form>
<form method="post" action="/admin/restore"><label>OAI 标识 <input name="oai_id" size="34" required></label>
<button class="ok" type="submit">恢复该记录</button></form>
<p class="muted">撤稿仅影响本来源；下一轮 ListRecords 会返回 status=deleted 的 header。</p></div>
<div class="card"><h3>当前状态</h3><pre>{state_json}</pre>
<h3>事件日志</h3><pre>{log_text}</pre></div>
<p><a href="/oai?verb=Identify">Identify</a> · <a href="/oai?verb=ListRecords&metadataPrefix=oai_dc">ListRecords 首页</a></p>
</main></body></html>"""


def make_handler(state: MockState):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"MockOAI/{state.code}"

        def log_message(self, fmt, *args):  # 精简访问日志
            state._log(f"HTTP {args[0] if args else ''}")

        def _send(self, status: int, ctype: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_slow(self, status: int, ctype: str, body: bytes) -> None:
            """慢正文：立即发响应头，正文按小块滴流发送（模拟慢机构库）。

            采集端取消时会硬中断连接，此处写失败（BrokenPipe）属预期。"""
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                for i in range(0, len(body), state.slow_chunk_bytes):
                    self.wfile.write(body[i:i + state.slow_chunk_bytes])
                    self.wfile.flush()
                    time.sleep(state.slow_chunk_seconds)
            except (BrokenPipeError, ConnectionResetError):
                state._log("客户端中断连接（采集被取消）")

        def _redirect_panel(self) -> None:
            self.send_response(303)
            self.send_header("Location", "/panel")
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlparse(self.path)
            qs = urllib.parse.parse_qs(parsed.query)
            if parsed.path == "/panel":
                return self._panel()
            if parsed.path != "/oai":
                return self._send(404, "text/plain; charset=utf-8", "not found".encode())
            return self._oai_get(qs)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlparse(self.path)
            length = int(self.headers.get("Content-Length", 0))
            form = urllib.parse.parse_qs(self.rfile.read(length).decode("utf-8"))
            with state.lock:
                if parsed.path == "/admin/expire-token":
                    state.token_expired = True
                    state._log("故障注入：下一次翻页令牌失效（一次性）")
                elif parsed.path == "/admin/expire-token-sticky":
                    state.token_expired_sticky = True
                    state._log("故障注入：令牌持续失效（直到重置）")
                elif parsed.path == "/admin/stream-break":
                    state.break_once = True
                    state._log("故障注入：下一次请求将断流")
                elif parsed.path == "/admin/slow-body":
                    state.slow_body_once = True
                    state._log("故障注入：下一次 ListRecords 响应慢正文（一次性）")
                elif parsed.path == "/admin/slow-body-sticky":
                    state.slow_body_sticky = True
                    state._log("故障注入：持续慢正文模式开启（每页滴流）")
                elif parsed.path == "/admin/slow-body-off":
                    state.slow_body_once = False
                    state.slow_body_sticky = False
                    state._log("慢正文模式已关闭")
                elif parsed.path == "/admin/reset":
                    state.reset()
                elif parsed.path in ("/admin/delete", "/admin/restore"):
                    oai_id = form.get("oai_id", [""])[0].strip()
                    target = next(
                        (r for r in state.records if r["oai_id"] == oai_id), None
                    )
                    if target is None:
                        state._log(f"撤稿/恢复失败：找不到 {oai_id}")
                    else:
                        target["deleted"] = parsed.path == "/admin/delete"
                        state._log(
                            f"{'撤稿' if target['deleted'] else '恢复'}：{oai_id}"
                        )
                else:
                    self._send(404, "text/plain", b"not found")
                    return
            self._redirect_panel()

        def _panel(self) -> None:
            with state.lock:
                active = sum(1 for r in state.records if not r["deleted"])
                snapshot = {
                    "source": state.code,
                    "records_total": len(state.records),
                    "records_active": active,
                    "records_deleted": len(state.records) - active,
                    "page_size": state.page_size,
                    "epoch": state.epoch,
                    "token_expired_one_shot": state.token_expired,
                    "token_expired_sticky": state.token_expired_sticky,
                    "stream_break_armed": state.break_once,
                    "slow_body_one_shot": state.slow_body_once,
                    "slow_body_sticky": state.slow_body_sticky,
                    "slow_chunk_seconds": state.slow_chunk_seconds,
                    "slow_chunk_bytes": state.slow_chunk_bytes,
                }
                page = PANEL_HTML.format(
                    code=state.code,
                    first_id=state.records[0]["oai_id"] if state.records else "",
                    state_json=html.escape(json.dumps(snapshot, ensure_ascii=False, indent=2)),
                    log_text=html.escape("\n".join(reversed(state.event_log)) or "（暂无）"),
                )
            self._send(200, "text/html; charset=utf-8", page.encode("utf-8"))

        def _oai_get(self, qs: dict[str, list[str]]) -> None:
            verb = (qs.get("verb") or [""])[0]
            with state.lock:
                if verb == "Identify":
                    body = (
                        "<request verb=\"Identify\">/oai</request>"
                        f"<Identify><repositoryName>Mock Repository {state.code}"
                        "</repositoryName><baseURL>http://localhost/oai</baseURL>"
                        "<protocolVersion>2.0</protocolVersion>"
                        "<adminEmail>lib@example.edu</adminEmail>"
                        "<earliestDatestamp>2020-01-01</earliestDatestamp>"
                        "<deletedRecord>persistent</deletedRecord>"
                        "<granularity>YYYY-MM-DD</granularity></Identify>"
                    )
                    return self._send(200, "text/xml; charset=utf-8", _oai_envelope(body))
                if verb != "ListRecords":
                    return self._send(200, "text/xml; charset=utf-8",
                                      _error_xml("/oai", "badVerb", f"不支持的 verb: {verb}"))

                token = (qs.get("resumptionToken") or [None])[0]
                # 故障注入只在「翻页请求」上触发，首页始终可用，便于稳定制造断点
                if token:
                    if state.break_once:
                        state.break_once = False
                        state._log("触发断流：返回 HTTP 500")
                        self._send(500, "text/plain; charset=utf-8",
                                   "simulated upstream stream failure".encode())
                        return
                    parsed_t = state.parse_token(token)
                    if (
                        parsed_t is None
                        or parsed_t[0] != state.epoch
                        or state.token_expired
                        or state.token_expired_sticky
                    ):
                        state.token_expired = False  # 一次性失效，重采后恢复
                        state._log(f"返回 badResumptionToken: {token}")
                        self._send(
                            200, "text/xml; charset=utf-8",
                            _error_xml("/oai", "badResumptionToken",
                                       "令牌已过期或不属于当前采集轮次"),
                        )
                        return
                    cursor = parsed_t[1]
                else:
                    if qs.get("metadataPrefix") != ["oai_dc"]:
                        return self._send(
                            200, "text/xml; charset=utf-8",
                            _error_xml("/oai", "badArgument",
                                       "首页必须提供 metadataPrefix=oai_dc"),
                        )
                    cursor = 0

                total = len(state.records)
                page = state.records[cursor:cursor + state.page_size]
                if not page and cursor != 0:
                    return self._send(
                        200, "text/xml; charset=utf-8",
                        _error_xml("/oai", "badResumptionToken", "游标超出范围"),
                    )
                next_cursor = cursor + len(page)
                token_xml = ""
                if next_cursor < total:
                    tok = state.make_token(next_cursor)
                    token_xml = (
                        f'<resumptionToken completeListSize="{total}" '
                        f'cursor="{next_cursor}">{tok}</resumptionToken>'
                    )
                body = (
                    "<request verb=\"ListRecords\">/oai</request><ListRecords>"
                    + "".join(_record_xml(r) for r in page)
                    + token_xml
                    + "</ListRecords>"
                )
                slow = state.slow_body_sticky or state.slow_body_once
                if state.slow_body_once:
                    state.slow_body_once = False  # 一次性慢正文，触发后复位
                state._log(f"ListRecords cursor={cursor} 返回 {len(page)} 条"
                           f"{'（慢正文滴流）' if slow else ''}")
            payload = _oai_envelope(body)
            if slow:
                return self._send_slow(200, "text/xml; charset=utf-8", payload)
            self._send(200, "text/xml; charset=utf-8", payload)

    return Handler


def serve(
    code: str,
    port: int,
    seed: int,
    count: int,
    registrant: str,
    page_size: int,
    overlap: int = 0,
    overlap_registrant: str | None = None,
    slow_body: bool = False,
    slow_chunk_seconds: float = 0.3,
    slow_chunk_bytes: int = 256,
) -> None:
    state = MockState(
        code, seed, count, registrant, page_size, overlap, overlap_registrant,
        slow_chunk_seconds=slow_chunk_seconds, slow_chunk_bytes=slow_chunk_bytes,
    )
    if slow_body:
        # 启动即进入持续慢正文模式：提供一个「持续慢发正文」的本地机构库
        state.slow_body_sticky = True
    httpd = ThreadingHTTPServer(("0.0.0.0", port), make_handler(state))
    httpd.daemon_threads = True
    print(f"Mock OAI source {code} listening on 0.0.0.0:{port} "
          f"({count} records, page_size={page_size}, overlap={overlap}"
          f"{', slow-body' if slow_body else ''})", flush=True)
    httpd.serve_forever()


def main() -> None:
    ap = argparse.ArgumentParser(description="本地 OAI-PMH 模拟机构库")
    ap.add_argument("--code", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--count", type=int, default=40)
    ap.add_argument("--registrant", default="1234")
    ap.add_argument("--page-size", type=int, default=10)
    ap.add_argument("--overlap", type=int, default=0,
                    help="与另一来源共用 DOI 的记录数（跨源去重演示）")
    ap.add_argument("--overlap-registrant", default=None,
                    help="共用 DOI 号段所属注册机构号")
    ap.add_argument("--slow-body", action="store_true",
                    help="启动即开启持续慢正文模式（每页滴流发送）")
    ap.add_argument("--slow-chunk-seconds", type=float, default=0.3,
                    help="慢正文每个分块之间的间隔秒数")
    ap.add_argument("--slow-chunk-bytes", type=int, default=256,
                    help="慢正文每个分块的字节数")
    args = ap.parse_args()
    serve(
        args.code, args.port, args.seed, args.count, args.registrant,
        args.page_size, args.overlap, args.overlap_registrant,
        slow_body=args.slow_body,
        slow_chunk_seconds=args.slow_chunk_seconds,
        slow_chunk_bytes=args.slow_chunk_bytes,
    )


if __name__ == "__main__":
    main()

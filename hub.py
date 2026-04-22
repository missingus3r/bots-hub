#!/usr/bin/env python3
"""bots-hub — shared message bus for Friday and Sam.

Puerto: 127.0.0.1:7788
DB:     ~/bots-hub/hub.db
Tokens: ~/bots-hub/tokens.json  (bot_name -> token)

Endpoints:
  GET  /health
  POST /ingest                    (X-Hub-Token required)
  GET  /messages?since=&chat_id=&limit=
  GET  /stream                    (SSE; optional X-Hub-Token)
  GET  /dashboard
"""

import json
import os
import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

VERSION = "0.1.0"
HOST = "0.0.0.0"
PORT = 7788
BASE = Path(os.path.expanduser("~/bots-hub"))
DB_PATH = BASE / "hub.db"
TOKENS_PATH = BASE / "tokens.json"

with open(TOKENS_PATH) as f:
    TOKENS = json.load(f)
TOKEN_TO_BOT = {v: k for k, v in TOKENS.items()}

_db_lock = threading.Lock()


def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


DB = db_connect()
DB.executescript("""
CREATE TABLE IF NOT EXISTS messages (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  chat_id     TEXT NOT NULL,
  msg_id      INTEGER,
  sender_id   TEXT,
  sender_name TEXT,
  is_bot      INTEGER DEFAULT 0,
  text        TEXT,
  ts          TEXT NOT NULL,
  ingested_at TEXT NOT NULL,
  reported_by TEXT NOT NULL,
  kind        TEXT DEFAULT 'incoming',
  raw         TEXT,
  UNIQUE(chat_id, msg_id, reported_by, kind)
);
CREATE INDEX IF NOT EXISTS idx_messages_ts   ON messages(ts);
CREATE INDEX IF NOT EXISTS idx_messages_chat ON messages(chat_id, ts);
""")


SUBSCRIBERS: list[queue.Queue] = []
SUBSCRIBERS_LOCK = threading.Lock()


def broadcast(event: dict):
    payload = json.dumps(event, ensure_ascii=False)
    with SUBSCRIBERS_LOCK:
        dead = []
        for q in SUBSCRIBERS:
            try:
                q.put_nowait(payload)
            except Exception:
                dead.append(q)
        for q in dead:
            SUBSCRIBERS.remove(q)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Handler(BaseHTTPRequestHandler):
    server_version = f"bots-hub/{VERSION}"

    def log_message(self, fmt, *args):
        return

    def _json(self, code: int, body):
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _auth_bot(self) -> str | None:
        tok = self.headers.get("X-Hub-Token", "").strip()
        return TOKEN_TO_BOT.get(tok)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)

        if u.path == "/health":
            return self._json(200, {"status": "ok", "version": VERSION})

        if u.path == "/messages":
            since = q.get("since", [None])[0]
            chat_id = q.get("chat_id", [None])[0]
            try:
                limit = min(int(q.get("limit", ["200"])[0]), 1000)
            except ValueError:
                limit = 200
            sql = "SELECT * FROM messages"
            conds, args = [], []
            if since:
                conds.append("ts > ?")
                args.append(since)
            if chat_id:
                conds.append("chat_id = ?")
                args.append(chat_id)
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            sql += " ORDER BY ts ASC LIMIT ?"
            args.append(limit)
            with _db_lock:
                rows = [dict(r) for r in DB.execute(sql, args).fetchall()]
            return self._json(200, {"messages": rows, "count": len(rows)})

        if u.path == "/stream":
            return self._sse()

        if u.path == "/dashboard" or u.path == "/":
            return self._dashboard()

        return self._json(404, {"error": "not found"})

    def do_POST(self):
        u = urlparse(self.path)
        if u.path != "/ingest":
            return self._json(404, {"error": "not found"})

        bot = self._auth_bot()
        if not bot:
            return self._json(401, {"error": "invalid or missing X-Hub-Token"})

        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return self._json(400, {"error": "invalid json"})

        chat_id = str(body.get("chat_id", ""))
        if not chat_id:
            return self._json(400, {"error": "chat_id required"})

        row = {
            "chat_id":     chat_id,
            "msg_id":      body.get("msg_id"),
            "sender_id":   body.get("sender_id"),
            "sender_name": body.get("sender_name"),
            "is_bot":      1 if body.get("is_bot") else 0,
            "text":        body.get("text", ""),
            "ts":          body.get("ts") or now_iso(),
            "ingested_at": now_iso(),
            "reported_by": bot,
            "kind":        body.get("kind", "incoming"),
            "raw":         json.dumps(body.get("raw"), ensure_ascii=False) if body.get("raw") is not None else None,
        }

        try:
            with _db_lock:
                cur = DB.execute("""
                    INSERT INTO messages
                    (chat_id, msg_id, sender_id, sender_name, is_bot, text, ts, ingested_at, reported_by, kind, raw)
                    VALUES (:chat_id, :msg_id, :sender_id, :sender_name, :is_bot, :text, :ts, :ingested_at, :reported_by, :kind, :raw)
                """, row)
                row_id = cur.lastrowid
            duplicate = False
        except sqlite3.IntegrityError:
            row_id = None
            duplicate = True

        event = {"type": "message", "row_id": row_id, "duplicate": duplicate, **row}
        if not duplicate:
            broadcast(event)

        return self._json(200, {"ok": True, "id": row_id, "duplicate": duplicate})

    def _sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()

        q: queue.Queue = queue.Queue(maxsize=512)
        with SUBSCRIBERS_LOCK:
            SUBSCRIBERS.append(q)
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            last_ping = time.time()
            while True:
                try:
                    payload = q.get(timeout=15)
                    self.wfile.write(f"event: message\ndata: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                except queue.Empty:
                    if time.time() - last_ping > 20:
                        self.wfile.write(b": ping\n\n")
                        self.wfile.flush()
                        last_ping = time.time()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with SUBSCRIBERS_LOCK:
                if q in SUBSCRIBERS:
                    SUBSCRIBERS.remove(q)

    def _dashboard(self):
        html = DASHBOARD_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.end_headers()
        self.wfile.write(html)


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>bots-hub</title>
<style>
  body { font-family: ui-monospace, Menlo, Consolas, monospace; background:#0b1020; color:#e4ecff; margin:0; padding:18px; }
  header { display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }
  h1 { margin:0; font-size:18px; color:#89b4fa; }
  .pill { background:#1e2a4a; padding:3px 10px; border-radius:12px; font-size:12px; }
  .pill.ok { background:#1e3a2b; color:#a6e3a1; }
  #stats { display:flex; gap:14px; margin:10px 0 18px; font-size:12px; color:#9ab0d8; }
  .stat b { color:#cdd6f4; }
  #msgs { display:flex; flex-direction:column-reverse; gap:6px; max-height:75vh; overflow:auto; background:#0f1630; border:1px solid #1e2a4a; border-radius:8px; padding:10px; }
  .msg { display:grid; grid-template-columns: 140px 80px 1fr; gap:10px; padding:6px 8px; border-bottom:1px solid #172043; font-size:13px; }
  .msg:last-child { border-bottom:none; }
  .ts { color:#7b88b3; font-size:11px; }
  .who { color:#fab387; font-size:11px; }
  .who.bot { color:#f5c2e7; }
  .by { color:#94e2d5; font-size:11px; }
  .txt { color:#e4ecff; white-space:pre-wrap; word-break:break-word; }
  .kind-outgoing { background:#0e1f2b; }
  .dup { opacity:0.55; }
</style>
</head>
<body>
<header>
  <h1>bots-hub <span class="pill" id="ver">·</span></h1>
  <span class="pill ok" id="conn">connecting…</span>
</header>
<div id="stats">
  <span class="stat">total: <b id="total">0</b></span>
  <span class="stat">last: <b id="last">-</b></span>
  <span class="stat">chats: <b id="chats">0</b></span>
</div>
<div id="msgs"></div>
<script>
const msgs = document.getElementById('msgs');
const stats = { total: 0, chats: new Set() };

function render(m, dup) {
  const row = document.createElement('div');
  row.className = 'msg kind-' + (m.kind || 'incoming') + (dup ? ' dup' : '');
  const ts = new Date(m.ts).toLocaleTimeString();
  const who = (m.sender_name || m.sender_id || '?') + (m.is_bot ? ' (bot)' : '');
  row.innerHTML =
    `<div><span class="ts">${ts}</span><br><span class="who ${m.is_bot?'bot':''}">${who}</span></div>` +
    `<div><span class="by">via ${m.reported_by}</span><br><span class="ts">${m.chat_id}</span></div>` +
    `<div class="txt"></div>`;
  row.querySelector('.txt').textContent = m.text || '';
  msgs.appendChild(row);
  stats.total++;
  stats.chats.add(m.chat_id);
  document.getElementById('total').textContent = stats.total;
  document.getElementById('last').textContent = ts;
  document.getElementById('chats').textContent = stats.chats.size;
}

async function boot() {
  try {
    const h = await fetch('/health').then(r => r.json());
    document.getElementById('ver').textContent = 'v' + h.version;
    const j = await fetch('/messages?limit=100').then(r => r.json());
    j.messages.forEach(m => render(m, false));
  } catch (e) { console.error(e); }

  const es = new EventSource('/stream');
  es.addEventListener('open', () => document.getElementById('conn').textContent = 'live');
  es.addEventListener('message', ev => {
    try { render(JSON.parse(ev.data), false); } catch (e) {}
  });
  es.onerror = () => document.getElementById('conn').textContent = 'reconnecting…';
}
boot();
</script>
</body>
</html>
"""


def main():
    srv = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"bots-hub v{VERSION} listening on http://{HOST}:{PORT}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()


if __name__ == "__main__":
    main()

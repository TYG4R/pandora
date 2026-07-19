#!/usr/bin/env python3
"""
PANDORA v3 — FastAPI + CDP screencast + Socket.IO live browser stream.
"""
import asyncio, base64, hashlib, io, json, os, re, sqlite3, time, traceback
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import socketio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from playwright.async_api import async_playwright

# ── sio ─────────────────────────────────────────────────────────────
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
app = FastAPI()
# Socket.IO ASGI app wraps FastAPI — /socket.io goes to Socket.IO, everything else to FastAPI
sio_asgi = socketio.ASGIApp(sio, app)

# ── DB ──────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "pandora.db")
ARTIFACT_DIR = os.path.join(os.path.dirname(__file__), "artifacts")

def init_db():
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL, title TEXT, depth INT DEFAULT 0,
            status INT DEFAULT 0, content_len INT DEFAULT 0, link_count INT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS bugs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_url TEXT NOT NULL, category TEXT NOT NULL,
            detail TEXT, snippet TEXT, screenshot_path TEXT, dom_snapshot TEXT
        );
    """)
    db.commit()
    return db

db = init_db()

def _hash_url(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]

def _save_artifact(url: str, screenshot: bytes) -> str:
    h = _hash_url(url)
    Path(ARTIFACT_DIR).mkdir(parents=True, exist_ok=True)
    p = os.path.join(ARTIFACT_DIR, f"{h}.png")
    with open(p, "wb") as f:
        f.write(screenshot)
    return p

def db_visit(url: str, title: str, depth: int, status: int, link_count: int):
    db.execute("INSERT OR REPLACE INTO pages (url, title, depth, status, link_count) VALUES (?,?,?,?,?)",
               (url, title[:500], depth, status, link_count))
    db.commit()

def db_bug(page_url: str, cat: str, detail: str, screenshot: Optional[bytes] = None):
    ss_path = _save_artifact(page_url, screenshot) if screenshot else ""
    db.execute("INSERT INTO bugs (page_url, category, detail, screenshot_path) VALUES (?,?,?,?)",
               (page_url, cat, detail[:500], ss_path))
    db.commit()

def db_stats():
    return dict(db.execute("SELECT (SELECT COUNT(*) FROM pages) AS pages, (SELECT COUNT(*) FROM bugs) AS bugs").fetchone())

def db_recent_bugs(limit: int = 50) -> list[dict]:
    return [dict(r) for r in db.execute("""
        SELECT b.page_url, b.category, b.detail, b.screenshot_path, p.title
        FROM bugs b LEFT JOIN pages p ON b.page_url = p.url
        ORDER BY b.rowid DESC LIMIT ?
    """, (limit,)).fetchall()]

# ── Config ──────────────────────────────────────────────────────────
MAX_PAGES = 500
MAX_DEPTH = 6
SAME_ORIGIN = "exact"
CONCURRENCY = 3
TIMEOUT_MS = 20000

def canonical(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path.rstrip('/') or '/'}"

def url_safe(url: str, base: str) -> bool:
    if not url.startswith(("http://", "https://")):
        return False
    ext = os.path.splitext(urlparse(url).path)[1].lower()
    if ext in {".pdf", ".zip", ".png", ".jpg", ".mp4", ".css", ".js"}:
        return False
    return urlparse(url).netloc == urlparse(base).netloc

# ── Crawler state ───────────────────────────────────────────────────
class CrawlState:
    def __init__(self):
        self.stop = False
        self.pages = 0
        self.bugs = 0
        self.errs = 0
        self.depth = 0
        self.t0 = 0.0
        self.root = ""
        self.visited: set = set()

state = CrawlState()

# ── Emit helpers ────────────────────────────────────────────────────
async def emit_stats(sid: str):
    s = db_stats()
    elapsed = time.time() - state.t0
    await sio.emit("stats", {
        "pages": s["pages"], "bugs": s["bugs"], "errors": state.errs,
        "depth": state.depth, "elapsed": f"{elapsed:.0f}s",
        "speed": f"{s['pages']/max(elapsed,1):.1f}",
        "queue": 0,
    }, room=sid)

async def emit_bugs(sid: str):
    await sio.emit("bugs", {"bugs": db_recent_bugs(20)}, room=sid)

# ── CDP Screencast ──────────────────────────────────────────────────
async def run_crawl(sid: str, start_url: str):
    state.stop = False
    state.pages = state.errs = state.depth = state.bugs = 0
    state.visited.clear()
    state.root = start_url
    state.t0 = time.time()

    # Clean old links
    db.execute("DELETE FROM bugs"); db.commit()
    db.execute("DELETE FROM pages"); db.commit()

    await sio.emit("log", "launching browser...", room=sid)

    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
    except Exception as e:
        await sio.emit("log", f"browser launch failed: {e}", room=sid)
        await sio.emit("done", {}, room=sid)
        return

    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Win; x64) AppleWebKit/537.36 Chrome/120 PANDORA/3.0",
        viewport={"width": 1280, "height": 720},
    )
    page = await ctx.new_page()

    # ── Start CDP screencast via Playwright CDP session ──
    cdp = await page.context.new_cdp_session(page)

    async def screencast_handler(params):
        nonlocal cdp
        if state.stop:
            return
        try:
            data = params.get("data", "")
            if data:
                await sio.emit("frame", {"data": data}, room=sid)
            # Must ack each frame or browser stops sending
            session_id = params.get("sessionId", 0)
            await cdp.send("Page.screencastFrameAck", {"sessionId": session_id})
        except Exception:
            pass

    cdp.on("Page.screencastFrame", lambda p: asyncio.ensure_future(screencast_handler(p)))

    try:
        await cdp.send("Page.startScreencast", {
            "format": "jpeg",
            "quality": 50,
            "maxWidth": 640,
            "maxHeight": 360,
            "everyNthFrame": 2,
        })
    except Exception as e:
        await sio.emit("log", f"screencast failed: {e}", room=sid)

    await sio.emit("log", "browser streaming live", room=sid)
    await emit_stats(sid)

    # ── Crawl loop ──
    norm = canonical(start_url)
    state.visited.add(norm)
    pending = [start_url]
    pending_depth = {start_url: 0}
    visited_local = {norm}

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async def process_one(url: str, depth: int):
        if state.stop or state.pages >= MAX_PAGES:
            return
        async with semaphore:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                await page.wait_for_timeout(2000)
            except Exception:
                pass

            title = await page.title()
            status_code = 200

            # Bug detection: HTTP status
            try:
                resp = await page.evaluate("() => document.readyState")
            except Exception:
                resp = "error"
                status_code = 500

            status = status_code

            # Extract links
            html = await page.content()
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            links = []
            for a in soup.find_all("a", href=True):
                h = a["href"].strip()
                if not h or h.startswith(("#", "javascript:", "mailto:", "tel:")):
                    continue
                f = urljoin(url, h)
                n = canonical(f)
                if n not in visited_local and url_safe(f, url) and len(visited_local) < MAX_PAGES:
                    visited_local.add(n)
                    links.append(f)

            nd = depth + 1
            for lk in links:
                if nd <= MAX_DEPTH and lk not in pending_depth:
                    pending_depth[lk] = nd
                    pending.append(lk)

            db_visit(url, title, depth, status, len(links))
            state.pages += 1
            state.depth = max(state.depth, depth)

            # Bugs
            if status >= 400:
                cat = "server_error" if status >= 500 else "client_error"
                try:
                    ss = await page.screenshot(type="png")
                except Exception:
                    ss = None
                db_bug(url, cat, f"HTTP {status}", screenshot=ss)

            await emit_stats(sid)
            await emit_bugs(sid)

    loop_start = time.time()
    while pending and not state.stop and state.pages < MAX_PAGES:
        batch = pending[:CONCURRENCY]
        pending = pending[CONCURRENCY:]
        tasks = [process_one(u, pending_depth[u]) for u in batch]
        await asyncio.gather(*tasks)

    # ── Stop screencast ──
    try:
        await cdp.send("Page.stopScreencast")
    except Exception:
        pass

    await page.close()
    await ctx.close()
    await browser.close()
    await pw.stop()

    await sio.emit("log", f"done — {state.pages} pages, {state.bugs} bugs", room=sid)
    await emit_stats(sid)
    await emit_bugs(sid)
    await sio.emit("done", {}, room=sid)

# ── Socket.IO events ────────────────────────────────────────────────
@sio.event
async def connect(sid, environ, auth):
    print(f"[connect] {sid}")

@sio.event
async def start(sid, data):
    url = data.get("url", "")
    if not url or not url.startswith("http"):
        await sio.emit("log", "invalid URL", room=sid)
        return
    asyncio.create_task(run_crawl(sid, url))

@sio.event
async def stop(sid, data):
    state.stop = True

@sio.event
async def disconnect(sid):
    print(f"[disconnect] {sid}")
    state.stop = True

# ── Frontend ────────────────────────────────────────────────────────
@app.get("/")
async def index():
    return HTMLResponse(FRONTEND_HTML)

FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PANDORA</title>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box}
@font-face{font-family:'SF Mono';src:local('SF Mono'),local('Menlo'),local('Consolas'),local('monospace')}
body{background:#0a0a0a;color:#ccc;font-family:'SF Mono','Fira Code',monospace;font-size:13px;height:100vh;display:flex;flex-direction:column;overflow:hidden}
header{background:#111;border-bottom:1px solid #222;padding:12px 20px;display:flex;align-items:center;gap:16px;flex-shrink:0}
h1{font-size:1.3rem;font-weight:900;letter-spacing:-.05em;text-transform:uppercase}
h1 em{color:#ff1744;font-style:normal}
.sub{color:#555;font-size:.6rem;margin-left:auto}
.input-row{display:flex;gap:8px;flex:1;max-width:600px}
.input-row input{flex:1;background:#1a1a1a;border:1px solid #333;border-radius:4px;padding:8px 12px;color:#ddd;font-family:inherit;font-size:.75rem;outline:none}
.input-row input:focus{border-color:#ff1744}
.input-row button{background:#ff1744;color:#fff;border:none;border-radius:4px;padding:8px 16px;font-family:inherit;font-size:.7rem;font-weight:700;cursor:pointer;text-transform:uppercase;letter-spacing:.05em}
.input-row button:disabled{opacity:.4;cursor:not-allowed}
.input-row button.devour{background:#ff1744}
.input-row button.stop{background:#555}
.main{display:flex;flex:1;overflow:hidden}
.video-panel{flex:1;display:flex;flex-direction:column;background:#0d0d0d;border-right:1px solid #222;min-width:0}
.video-wrap{flex:1;display:flex;align-items:center;justify-content:center;overflow:hidden;position:relative;background:repeating-conic-gradient(#111 0% 25%,#161616 0% 50%) 0 0/20px 20px}
.video-wrap img{max-width:100%;max-height:100%;display:block;image-rendering:auto}
.video-wrap .placeholder{color:#333;font-size:.7rem;letter-spacing:.1em;text-transform:uppercase}
.sidebar{width:340px;display:flex;flex-direction:column;background:#0d0d0d;flex-shrink:0}
.stats-grid{display:grid;grid-template-columns:1fr 1fr;gap:0;border-bottom:1px solid #222}
.stat{border-bottom:1px solid #1a1a1a;padding:10px 14px}
.stat:not(:nth-child(2n)){border-right:1px solid #1a1a1a}
.stat .l{font-size:.55rem;color:#555;text-transform:uppercase;letter-spacing:.08em}
.stat .v{font-size:.85rem;font-weight:700;margin-top:2px}
.stat .v.g{color:#00e676}.stat .v.r{color:#ff1744}.stat .v.a{color:#ffab00}
.log-box{flex:1;overflow-y:auto;padding:10px 14px;font-size:.6rem;color:#555;line-height:1.6;border-bottom:1px solid #222}
.log-box .log-entry{color:#555}
.log-box .log-info{color:#448aff}
.log-box .log-done{color:#00e676}
.bugs-box{max-height:200px;overflow-y:auto;padding:10px 14px;font-size:.6rem}
.bugs-box .bug-title{color:#ffab00;margin-bottom:4px}
.bugs-box .bug-empty{color:#333;text-align:center;padding:1rem}
#startBtn:disabled{opacity:.4}
#statusBar{font-size:.55rem;color:#333;text-align:center;padding:6px;border-top:1px solid #1a1a1a;flex-shrink:0}
</style>
</head>
<body>
<header>
<h1><em>P</em>ANDORA</h1>
<div class="input-row">
<input id="urlInput" value="https://example.com" placeholder="target URL">
<button id="startBtn" class="devour" onclick="startCrawl()">DEVOUR</button>
<button id="stopBtn" class="stop" onclick="stopCrawl()" style="display:none">STOP</button>
</div>
<div class="sub">cdp screencast · live stream</div>
</header>
<div class="main">
<div class="video-panel">
<div class="video-wrap" id="videoWrap">
<img id="liveFrame" style="display:none">
<div class="placeholder" id="placeholder">awaiting first page</div>
</div>
</div>
<div class="sidebar">
<div class="stats-grid" id="statsGrid">
<div class="stat"><div class="l">Pages</div><div class="v g" id="pages">0</div></div>
<div class="stat"><div class="l">Bugs</div><div class="v a" id="bugs">0</div></div>
<div class="stat"><div class="l">Errors</div><div class="v r" id="errors">0</div></div>
<div class="stat"><div class="l">Depth</div><div class="v" id="depth">0</div></div>
<div class="stat"><div class="l">Speed</div><div class="v" id="speed">0</div></div>
<div class="stat"><div class="l">Elapsed</div><div class="v" id="elapsed">0s</div></div>
</div>
<div class="log-box" id="logBox"><div class="log-entry">ready.</div></div>
<div class="bugs-box" id="bugsBox"><div class="bug-empty">no bugs yet</div></div>
<div id="statusBar">idle</div>
</div>
</div>
<script>
const socket = io({ transports: ["websocket"], upgrade: false });
let running = false;
const $ = id => document.getElementById(id);

function addLog(msg, cls="log-entry") {
  const el = document.createElement("div");
  el.className = cls;
  el.textContent = msg;
  const box = $("logBox");
  box.appendChild(el);
  box.scrollTop = box.scrollHeight;
}

function updateStats(d) {
  $("pages").textContent = d.pages || 0;
  $("bugs").textContent = d.bugs || 0;
  $("errors").textContent = d.errors || 0;
  $("depth").textContent = d.depth || 0;
  $("speed").textContent = d.speed || "0";
  $("elapsed").textContent = d.elapsed || "0s";
}

function renderBugs(bugs) {
  const box = $("bugsBox");
  if (!bugs || bugs.length === 0) {
    box.innerHTML = '<div class="bug-empty">no bugs yet</div>';
    return;
  }
  box.innerHTML = bugs.slice(0, 20).map(b => {
    const cat = b.category || "?";
    const url = (b.page_url || "").split("//").pop() || "";
    const detail = (b.detail || "").slice(0, 120);
    return `<div class="bug-title">[${cat}] ${url}</div><div style="color:#666;margin-bottom:8px">${detail}</div>`;
  }).join("");
}

function startCrawl() {
  const url = $("urlInput").value.trim();
  if (!url || running) return;
  running = true;
  $("startBtn").disabled = true;
  $("stopBtn").style.display = "inline";
  $("logBox").innerHTML = "";
  $("bugsBox").innerHTML = '<div class="bug-empty">waiting...</div>';
  $("statusBar").textContent = "crawling...";
  $("placeholder").style.display = "block";
  $("placeholder").textContent = "launching browser...";
  $("liveFrame").style.display = "none";
  socket.emit("start", { url });
}

function stopCrawl() {
  socket.emit("stop", {});
  running = false;
  $("startBtn").disabled = false;
  $("stopBtn").style.display = "none";
  $("statusBar").textContent = "stopped";
}

socket.on("frame", data => {
  if (data.data) {
    $("liveFrame").src = "data:image/jpeg;base64," + data.data;
    $("liveFrame").style.display = "block";
    $("placeholder").style.display = "none";
  }
});

socket.on("log", msg => {
  addLog(msg, "log-info");
});

socket.on("stats", updateStats);

socket.on("bugs", data => {
  renderBugs(data.bugs);
});

socket.on("done", () => {
  running = false;
  $("startBtn").disabled = false;
  $("stopBtn").style.display = "none";
  $("statusBar").textContent = "done";
  addLog("crawl complete", "log-done");
});

socket.on("disconnect", () => {
  addLog("disconnected", "log-done");
});
</script>
</body>
</html>"""

# ── entry ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 7860))
    uvicorn.run("main:sio_asgi", host="0.0.0.0", port=port)

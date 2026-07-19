#!/usr/bin/env python3
"""
PANDORA v4 — FastAPI + CDP screencast + brutal crawler + auth/captcha + CSV export.
"""
import asyncio, base64, csv, hashlib, io, json, os, re, sqlite3, time, traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

import socketio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response
from playwright.async_api import async_playwright, TimeoutError as PwTimeout

# ── config ────────────────────────────────────────────────────────────
CONCURRENCY = int(os.environ.get("CONCURRENCY", "5"))
MAX_PAGES = int(os.environ.get("MAX_PAGES", "500"))
MAX_DEPTH = int(os.environ.get("MAX_DEPTH", "10"))
FOLLOW_SUBDOMAINS = os.environ.get("FOLLOW_SUBDOMAINS", "1") == "1"
CRAWL_TIMEOUT = int(os.environ.get("CRAWL_TIMEOUT", "300"))

# ── sio ───────────────────────────────────────────────────────────────
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*", max_http_data_size=5*1024*1024)
fastapi_app = FastAPI()
app = fastapi_app
sio_asgi = socketio.ASGIApp(sio, fastapi_app)

# ── DB ────────────────────────────────────────────────────────────────
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
        CREATE TABLE IF NOT EXISTS credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT NOT NULL, page_url TEXT, login_selector TEXT,
            username_field TEXT, password_field TEXT,
            username_val TEXT, password_val TEXT
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

def db_bug(page_url: str, cat: str, detail: str, snippet: str = "",
           screenshot: Optional[bytes] = None, dom_snapshot: str = ""):
    spath = _save_artifact(page_url, screenshot) if screenshot else ""
    db.execute("INSERT INTO bugs (page_url, category, detail, snippet, screenshot_path, dom_snapshot) VALUES (?,?,?,?,?,?)",
               (page_url, cat, detail[:500], snippet[:1000], spath, dom_snapshot[:2000]))
    db.commit()
    return db.execute("SELECT last_insert_rowid()").fetchone()[0]

def db_stats():
    row = db.execute("SELECT COUNT(*) as pages, SUM(CASE WHEN status>0 THEN 1 ELSE 0 END) as visited FROM pages").fetchone()
    bugs = db.execute("SELECT COUNT(*) as c FROM bugs").fetchone()
    return {"pages": row["pages"], "visited": row["visited"] or 0, "bugs": bugs["c"]}

def db_recent_bugs(limit: int = 50) -> list[dict]:
    return [dict(r) for r in db.execute("SELECT * FROM bugs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]

def db_export_csv() -> str:
    rows = db.execute("SELECT * FROM bugs ORDER BY id").fetchall()
    if not rows:
        return "no bugs found"
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(rows[0].keys())
    for r in rows:
        w.writerow([str(v) if v else "" for v in r])
    return out.getvalue()

# ── helpers ───────────────────────────────────────────────────────────
def canonical(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}{p.path}".rstrip("/") or f"{p.scheme}://{p.netloc}"

def url_safe(url: str, base: str) -> bool:
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.netloc:
            return False
        if "#" in p.fragment or "javascript:" in url:
            return False
        ext = Path(p.path).suffix.lower()
        if ext in (".pdf", ".zip", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".mp4", ".mp3"):
            return False
        return True
    except Exception:
        return False

def same_domain(url: str, base: str) -> bool:
    return urlparse(url).netloc == urlparse(base).netloc

# ── Crawl State ───────────────────────────────────────────────────────
class CrawlState:
    def __init__(self):
        self.reset()

    def reset(self):
        self.running = False
        self.paused = False
        self.pending = []
        self.seen = set()
        self.pages_visited = 0
        self.depth = 0
        self.start_url = ""
        self.start_domain = ""
        self.start_ts = 0.0
        self.bugs = []
        self.last_pages_batch = []
        self.creds = []
        self.waiting_captcha = False
        self.captcha_resolved = asyncio.Event()
        self.captcha_resolved.set()
        self.lock = asyncio.Lock()
        self.launch_opts = {"headless": True, "args": [
            "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"
        ]}

_state = CrawlState()

# ── socket events ─────────────────────────────────────────────────────
async def emit_state(sid: str):
    stats = db_stats()
    elapsed = time.time() - _state.start_ts if _state.start_ts else 0
    await sio.emit("state", {
        "pages": _state.pages_visited,
        "total_pages": max(_state.pages_visited, stats["pages"]),
        "bugs": stats["bugs"],
        "depth": _state.depth,
        "speed": round(_state.pages_visited / elapsed, 1) if elapsed > 0 else 0,
        "elapsed": round(elapsed),
        "running": _state.running,
        "paused": _state.paused,
        "waiting_captcha": _state.waiting_captcha,
        "pending": len(_state.pending),
    }, to=sid)

async def emit_log(sid: str, msg: str, cls: str = "log-info"):
    await sio.emit("log", {"msg": msg, "cls": cls}, to=sid)

async def emit_bugs(sid: str):
    bugs = db_recent_bugs(10)
    await sio.emit("bugs", bugs, to=sid)

@sio.event
async def connect(sid, environ, auth):
    await emit_state(sid)
    await emit_log(sid, "connected", "log-done")
    # send stored creds
    creds = [dict(r) for r in db.execute("SELECT * FROM credentials").fetchall()]
    await sio.emit("credentials", creds, to=sid)

@sio.event
async def request_state(sid, data):
    await emit_state(sid)

@sio.event
async def serve_bugs_csv(sid, data):
    """Send CSV export over the socket as a text blob."""
    csv_data = db_export_csv()
    await sio.emit("csv_export", csv_data, to=sid)

@sio.event
async def save_credential(sid, data):
    db.execute("INSERT INTO credentials (label, page_url, login_selector, username_field, password_field, username_val, password_val) VALUES (?,?,?,?,?,?,?)",
               (data.get("label",""), data.get("page_url",""), data.get("login_selector",""),
                data.get("username_field",""), data.get("password_field",""),
                data.get("username_val",""), data.get("password_val","")))
    db.commit()
    await emit_log(sid, f"credential '{data.get('label','')}' saved", "log-done")
    creds = [dict(r) for r in db.execute("SELECT * FROM credentials").fetchall()]
    await sio.emit("credentials", creds, to=sid)

@sio.event
async def delete_credential(sid, data):
    db.execute("DELETE FROM credentials WHERE id=?", (data.get("id"),))
    db.commit()
    creds = [dict(r) for r in db.execute("SELECT * FROM credentials").fetchall()]
    await sio.emit("credentials", creds, to=sid)

@sio.event
async def captcha_done(sid, data):
    _state.captcha_resolved.set()
    _state.waiting_captcha = False
    await emit_log(sid, "captcha resolved, resuming", "log-done")
    await emit_state(sid)

@sio.event
async def start(sid, data):
    if _state.running:
        await emit_log(sid, "already running", "log-warn")
        return
    _state.reset()
    _state.running = True
    _state.start_url = data.get("url", "")
    _state.start_ts = time.time()
    _state.start_domain = urlparse(_state.start_url).netloc
    _state.pending = [(urlparse(_state.start_url)._replace(fragment="").geturl(), 0)]
    _state.launch_opts["headless"] = data.get("headless", True)
    await emit_log(sid, f"starting crawl: {_state.start_url}", "log-start")
    await emit_state(sid)
    asyncio.create_task(_run_crawl(sid, data))

@sio.event
async def stop(sid, data):
    _state.running = False
    _state.paused = False
    await emit_log(sid, "stopped by user", "log-warn")
    await emit_state(sid)

@sio.event
async def pause(sid, data):
    _state.paused = True
    await emit_log(sid, "paused", "log-warn")
    await emit_state(sid)

@sio.event
async def resume(sid, data):
    _state.paused = False
    await emit_log(sid, "resumed", "log-start")
    await emit_state(sid)

@sio.event
async def disconnect(sid):
    pass

# ── crawl logic ───────────────────────────────────────────────────────
async def _process_page(page, url: str, depth: int, sid: str, browser) -> list:
    """Visit a URL, extract links, check for bugs. Returns [(url, depth), ...]."""
    result_links = []
    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
    except PwTimeout:
        try:
            await page.goto(url, wait_until="load", timeout=15000)
        except Exception as e:
            await emit_log(sid, f"timeout: {url[:80]}", "log-warn")
            db_visit(url, "", depth, -1, 0)
            return []

    title = await page.title()
    content = await page.content()
    text = await page.inner_text("body") if await page.query_selector("body") else ""
    bt = text.strip()
    content_len = len(bt)
    screenshot = await page.screenshot(full_page=False, type="png")

    # bug detection via heuristics
    bugs_found = []
    lower = bt.lower()

    # 404 / error pages
    if re.search(r"\b(404|not found|page not found|oops)\b", lower):
        bugs_found.append(("404 Error", f"page returns 404: {title}", "", ""))
    if re.search(r"\b(500|internal server error|application error)\b", lower):
        bugs_found.append(("5xx Error", f"server error: {title}", "", ""))

    # broken images
    broken_imgs = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('img')).filter(i => {
            if (!i.complete) return false;
            return i.naturalWidth === 0 || i.naturalHeight === 0;
        }).length;
    }""")
    if broken_imgs > 0:
        bugs_found.append(("Broken Images", f"{broken_imgs} broken images", "", ""))

    # console errors
    console_errors = await page.evaluate("""() => {
        return window.__pandora_errors || [];
    }""") if hasattr(page, '_injected') else 0
    if console_errors:
        bugs_found.append(("Console Errors", f"{console_errors} console errors", "", ""))

    # insecure forms
    forms = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('form[action^="http:"]')).map(f => f.action);
    }""")
    if forms:
        bugs_found.append(("Insecure Forms", f"{len(forms)} forms submit over HTTP", "", ""))

    # accessibility: missing alt text
    missing_alt = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('img:not([alt])')).length;
    }""")
    if missing_alt > 5:
        bugs_found.append(("Accessibility", f"{missing_alt} images missing alt text", "", ""))

    # store bugs
    for cat, detail, snippet, dom in bugs_found:
        db_bug(url, cat, detail, snippet, screenshot, dom)
        await emit_log(sid, f"  [{cat}] {url[:60]}", "log-bug")
        _state.bugs.append(cat)

    db_visit(url, title, depth, 1, 0)
    _state.pages_visited += 1
    _state.depth = max(_state.depth, depth)

    # emit screenshot frame
    b64 = base64.b64encode(screenshot).decode()
    await sio.emit("frame", {"image": b64, "url": url, "title": title}, to=sid)
    await emit_state(sid)

    # extract links
    links = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('a[href]')).map(a => a.href);
    }""")
    base_url = url
    base_domain = self_domain = urlparse(url).netloc

    for link in links:
        resolved = urljoin(base_url, link)
        if not url_safe(resolved, base_url):
            continue
        can = canonical(resolved)
        if can in _state.seen:
            continue
        link_domain = urlparse(resolved).netloc
        if link_domain != base_domain:
            if not FOLLOW_SUBDOMAINS:
                continue
            # allow subdomains of the start domain
            if not link_domain.endswith("." + _state.start_domain) and link_domain != _state.start_domain:
                continue
        _state.seen.add(can)
        result_links.append((resolved, depth + 1))

    return result_links

async def _try_login(page, url: str, sid: str):
    """Attempt credential-based login if credentials match this URL."""
    for cred in _state.creds:
        if cred["page_url"] and cred["page_url"] not in url:
            continue
        try:
            if cred.get("login_selector"):
                btn = await page.query_selector(cred["login_selector"])
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(2000)
            if cred.get("username_field"):
                await page.fill(cred["username_field"], cred.get("username_val",""))
            if cred.get("password_field"):
                await page.fill(cred["password_field"], cred.get("password_val",""))
                await page.press(cred["password_field"], "Enter")
                await page.wait_for_timeout(3000)
            await emit_log(sid, f"logged in with '{cred.get('label','')}' on {url[:50]}...", "log-start")
            return True
        except Exception as e:
            await emit_log(sid, f"login failed for '{cred.get('label','')}': {str(e)[:60]}", "log-warn")
    return False

async def _detect_captcha(page, sid: str) -> bool:
    """Detect captcha via strict indicators.  Returns True if captcha is confirmed."""
    # Quick check for recaptcha/hcaptcha badge/iframe (the real thing)
    try:
        has_recaptcha = await page.evaluate("""() => {
            const els = document.querySelectorAll('script[src*="recaptcha"], .g-recaptcha, iframe[src*="recaptcha/api"], iframe[src*="hcaptcha"]');
            return els.length > 0;
        }""")
        if has_recaptcha:
            # Double-check: a visible recaptcha iframe means it's actually challenging
            visible = await page.evaluate("""() => {
                const ifr = document.querySelector('iframe[src*="recaptcha/api"], iframe[src*="hcaptcha"]');
                if (!ifr) return false;
                const rect = ifr.getBoundingClientRect();
                return rect.width > 0 && rect.height > 0;
            }""")
            if not visible:
                # recaptcha script loaded but badge not visible — not blocking
                return False
            _state.waiting_captcha = True
            _state.captcha_resolved.clear()
            await emit_log(sid, "⚠ reCAPTCHA detected — waiting for human to solve", "log-bug")
            await sio.emit("captcha_needed", {
                "url": page.url,
                "screenshot": base64.b64encode(await page.screenshot()).decode()
            }, to=sid)
            await emit_state(sid)
            # Wait up to 120s for human to solve
            try:
                await asyncio.wait_for(_state.captcha_resolved.wait(), timeout=120)
            except asyncio.TimeoutError:
                await emit_log(sid, "captcha timeout — skipping page", "log-warn")
            return True
    except Exception:
        pass
    return False
    return False

async def _inject_monitor(page):
    """Inject console error collector."""
    await page.evaluate("""() => { window.__pandora_errors = 0; }""")
    await page.evaluate("""() => {
        const orig = console.error;
        console.error = function() {
            window.__pandora_errors = (window.__pandora_errors || 0) + 1;
            return orig.apply(console, arguments);
        };
    }""")

async def _run_crawl(sid: str, data):
    sem = asyncio.Semaphore(CONCURRENCY)
    browser = None

    try:
        p = await async_playwright().__aenter__()
        browser = await p.chromium.launch(**_state.launch_opts)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

        # load saved credentials
        _state.creds = [dict(r) for r in db.execute("SELECT * FROM credentials").fetchall()]

        await emit_log(sid, "browser launched", "log-done")

        async def worker(url: str, depth: int):
            async with sem:
                if not _state.running:
                    return []
                page = await context.new_page()
                await _inject_monitor(page)
                try:
                    # detect captcha before processing
                    await page.goto(url, wait_until="load", timeout=30000)
                    captcha = await _detect_captcha(page, sid)
                    if captcha:
                        page2 = await context.new_page()
                        await _inject_monitor(page2)
                        links = await _process_page(page2, url, depth, sid, browser)
                        await page2.close()
                        return links
                    # try login if credentials exist
                    await _try_login(page, url, sid)
                    links = await _process_page(page, url, depth, sid, browser)
                    return links
                except Exception as e:
                    await emit_log(sid, f"error: {url[:60]} - {str(e)[:60]}", "log-warn")
                    return []
                finally:
                    await page.close()

        while _state.running and _state.pending and _state.pages_visited < MAX_PAGES and _state.depth < MAX_DEPTH:
            while _state.paused and _state.running:
                await asyncio.sleep(1)
            # check timeout
            if time.time() - _state.start_ts > CRAWL_TIMEOUT:
                await emit_log(sid, f"timeout ({CRAWL_TIMEOUT}s) reached", "log-warn")
                break

            batch = []
            async with _state.lock:
                while _state.pending and len(batch) < CONCURRENCY * 2:
                    url, depth = _state.pending.pop(0)
                    if depth > MAX_DEPTH:
                        continue
                    can = canonical(url)
                    if can in _state.seen:
                        continue
                    _state.seen.add(can)
                    batch.append((url, depth))

            if not batch:
                if not _state.pending:
                    break
                continue

            tasks = [worker(url, d) for url, d in batch]
            results = await asyncio.gather(*tasks)
            for links in results:
                async with _state.lock:
                    _state.pending.extend(links)

        await emit_log(sid, "crawl complete", "log-done")
        await sio.emit("crawl_complete", {}, to=sid)

    except Exception as e:
        await emit_log(sid, f"crawl error: {str(e)[:200]}", "log-bug")
        traceback.print_exc()
    finally:
        _state.running = False
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        await emit_state(sid)

# ── routes ─────────────────────────────────────────────────────────────
@app.get("/")
async def index():
    return HTMLResponse(FRONTEND_HTML)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/export.csv")
async def export_csv():
    csv_data = db_export_csv()
    return Response(content=csv_data, media_type="text/csv",
                    headers={"Content-Disposition": "attachment; filename=pandora_bugs.csv"})

FRONTEND_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>PANDORA</title>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<style>
*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}
:root{--bg:#0a0a0a;--surface:#111;--border:#222;--text:#ccc;--dim:#666;--accent:#0f0;--bug:#f44;--warn:#fa0;--radius:8px}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,sans-serif;font-size:14px;min-height:100dvh;overflow-x:hidden}
header{background:var(--surface);border-bottom:1px solid var(--border);padding:10px 12px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:10;flex-shrink:0}
header h1{font-size:15px;font-weight:700;color:var(--accent);letter-spacing:1px}
header .sub{color:var(--dim);font-size:11px;margin-left:auto}
.panel{padding:8px 12px}
input,select,button{font:inherit;border-radius:var(--radius);border:1px solid var(--border);background:var(--surface);color:var(--text);padding:8px 10px;font-size:13px}
input:focus,select:focus{outline:none;border-color:var(--accent)}
button{cursor:pointer;font-weight:600;transition:.15s;white-space:nowrap;user-select:none;-webkit-user-select:none}
button:active{transform:scale(.97)}
.btn-primary{background:var(--accent);color:#000;border-color:var(--accent)}
.btn-danger{background:var(--bug);color:#fff;border-color:var(--bug)}
.btn-warn{background:var(--warn);color:#000;border-color:var(--warn)}
.btn-outline{background:transparent;border-color:var(--border)}
.btn-sm{padding:5px 8px;font-size:11px}
.btn-icon{width:36px;height:36px;padding:0;display:inline-flex;align-items:center;justify-content:center;font-size:16px}
.url-row{display:flex;gap:6px;width:100%}.url-row input{flex:1;min-width:0}
.stats-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:8px}
.stat{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:8px 4px;text-align:center}
.stat .val{font-size:17px;font-weight:700;color:var(--accent)}
.stat .lbl{font-size:10px;color:var(--dim);margin-top:2px}
.video-wrap{position:relative;width:100%;aspect-ratio:16/9;background:#000;border-radius:var(--radius);overflow:hidden;border:1px solid var(--border);margin-bottom:8px}
.video-wrap img{width:100%;height:100%;object-fit:contain;display:block}
.video-overlay{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;color:#888;font-size:12px;pointer-events:none;background:rgba(0,0,0,.5);text-align:center;padding:10px;word-break:break-all}
.log-box{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:8px;max-height:160px;overflow-y:auto;font-family:monospace;font-size:11px;line-height:1.5}
.log-box .log-start{color:var(--accent)}
.log-box .log-bug{color:var(--bug)}
.log-box .log-warn{color:var(--warn)}
.log-box .log-done{color:var(--dim)}
.bug-list{display:flex;flex-direction:column;gap:4px;max-height:200px;overflow-y:auto}
.bug-item{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:6px 8px;font-size:11px;display:flex;justify-content:space-between;align-items:start;gap:6px}
.bug-item .cat{color:var(--bug);font-weight:600;white-space:nowrap;flex-shrink:0}
.bug-item .url{color:var(--dim);word-break:break-all}
.action-row{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.85);z-index:100;display:none;align-items:center;justify-content:center;padding:16px}
.modal-overlay.open{display:flex}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);max-width:440px;width:100%;max-height:85vh;overflow-y:auto;padding:16px}
.modal h3{font-size:14px;margin-bottom:12px}
.modal .form-group{margin-bottom:10px}
.modal .form-group label{display:block;font-size:11px;color:var(--dim);margin-bottom:3px}
.modal .form-group input,.modal .form-group select{width:100%}
.modal-actions{display:flex;gap:8px;margin-top:12px;justify-content:flex-end}
.tab-bar{display:flex;border-bottom:1px solid var(--border);margin-bottom:8px}
.tab{padding:8px 14px;font-size:12px;cursor:pointer;border-bottom:2px solid transparent;color:var(--dim);font-weight:600;background:none;border-radius:0}
.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.tab-content{display:none}.tab-content.active{display:block}
@media(max-width:400px){
  .stats-grid{gap:4px}
  .stat .val{font-size:14px}
  .stat{padding:6px 2px}
  header{padding:8px}
  .panel{padding:6px 8px}
  .log-box{max-height:120px;font-size:10px}
}
</style>
</head>
<body>
<header>
  <h1>PANDORA</h1>
  <span class="sub" id="statusBadge">idle</span>
</header>

<div class="panel">
  <div class="url-row">
    <input type="url" id="urlInput" placeholder="https://example.com" autocomplete="url" enterkeyhint="go">
    <button class="btn-primary" id="startBtn">Go</button>
    <button class="btn-outline btn-icon" id="pauseBtn" style="display:none">⏸</button>
    <button class="btn-danger btn-icon" id="stopBtn" style="display:none">■</button>
  </div>
</div>

<div class="panel" style="padding-top:0">
  <div class="stats-grid" id="statsGrid">
    <div class="stat"><div class="val" id="statPages">0</div><div class="lbl">Pages</div></div>
    <div class="stat"><div class="val" id="statBugs" style="color:var(--bug)">0</div><div class="lbl">Bugs</div></div>
    <div class="stat"><div class="val" id="statDepth">0</div><div class="lbl">Depth</div></div>
    <div class="stat"><div class="val" id="statSpeed">0</div><div class="lbl">Pg/min</div></div>
    <div class="stat"><div class="val" id="statPending">0</div><div class="lbl">Queue</div></div>
    <div class="stat"><div class="val" id="statElapsed">0s</div><div class="lbl">Time</div></div>
  </div>

  <div class="video-wrap" id="videoWrap">
    <img id="liveFrame" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=" alt="">
    <div class="video-overlay" id="videoOverlay">waiting...</div>
  </div>

  <div class="action-row">
    <button class="btn-outline btn-sm" id="csvBtn">⬇ CSV</button>
    <button class="btn-outline btn-sm" id="credsBtn">🔑 Auth</button>
    <button class="btn-outline btn-sm" id="settingsBtn">⚙</button>
  </div>

  <div class="tab-bar">
    <div class="tab active" data-tab="logs">Logs</div>
    <div class="tab" data-tab="bugs">Bugs</div>
  </div>

  <div class="tab-content active" id="tabLogs">
    <div class="log-box" id="logBox"><div style="color:var(--dim)">start a crawl to see logs</div></div>
  </div>
  <div class="tab-content" id="tabBugs">
    <div class="bug-list" id="bugList"><div style="color:var(--dim);font-size:12px">no bugs found</div></div>
  </div>
</div>

<div class="modal-overlay" id="credsModal">
  <div class="modal">
    <h3 style="color:var(--accent)">🔑 Credentials</h3>
    <div id="credsList" style="margin-bottom:10px;font-size:12px;color:var(--dim)">none saved</div>
    <hr style="border-color:var(--border);margin:10px 0">
    <div class="form-group"><label>Label</label><input id="credLabel" placeholder="Admin Login"></div>
    <div class="form-group"><label>Page URL (blank = all)</label><input id="credUrl" placeholder="https://site.com/login"></div>
    <div class="form-group"><label>Login selector</label><input id="credLoginSel" placeholder="button[type=submit]"></div>
    <div class="form-group"><label>Username field</label><input id="credUserField" placeholder="#username"></div>
    <div class="form-group"><label>Password field</label><input id="credPassField" placeholder="#password"></div>
    <div class="form-group"><label>Username</label><input id="credUserVal" placeholder="admin"></div>
    <div class="form-group"><label>Password</label><input id="credPassVal" type="password" placeholder="••••"></div>
    <div class="modal-actions">
      <button class="btn-outline btn-sm" id="credsCloseBtn">Close</button>
      <button class="btn-primary btn-sm" id="credsSaveBtn">Save</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="settingsModal">
  <div class="modal">
    <h3 style="color:var(--accent)">⚙ Settings</h3>
    <div class="form-group"><label>Max Pages</label><input id="setMaxPages" type="number" value="500"></div>
    <div class="form-group"><label>Max Depth</label><input id="setMaxDepth" type="number" value="10"></div>
    <div class="form-group"><label>Concurrency</label><input id="setConcurrency" type="number" value="5"></div>
    <div class="form-group"><label>Follow Subdomains</label><select id="setFollowSub"><option value="1">Yes</option><option value="0">No</option></select></div>
    <div class="form-group"><label>Timeout (sec)</label><input id="setTimeout" type="number" value="300"></div>
    <div class="modal-actions">
      <button class="btn-outline btn-sm" id="settingsCloseBtn">Close</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="captchaModal">
  <div class="modal">
    <h3 style="color:var(--warn)">⚠ CAPTCHA</h3>
    <p style="font-size:12px;color:var(--dim);margin-bottom:10px">Solve the captcha, then click Resume.</p>
    <img id="captchaScreenshot" style="width:100%;border-radius:var(--radius);border:1px solid var(--border);margin-bottom:10px">
    <div class="modal-actions"><button class="btn-primary" id="captchaResumeBtn">✓ Resume</button></div>
  </div>
</div>

<script>
const socket = io({transports:["websocket","polling"]});
const $ = id => document.getElementById(id);

socket.on("connect", () => addLog("connected", "log-done"));

socket.on("state", s => {
  $("statPages").textContent = s.pages || 0;
  $("statBugs").textContent = s.bugs || 0;
  $("statDepth").textContent = s.depth || 0;
  $("statSpeed").textContent = s.speed || 0;
  $("statPending").textContent = s.pending || 0;
  $("statElapsed").textContent = (s.elapsed||0)+"s";
  $("statusBadge").textContent = s.running ? (s.paused ? "paused" : "crawling") : "idle";
  if (s.waiting_captcha) $("statusBadge").textContent = "⚠ captcha";
  $("pauseBtn").textContent = s.paused ? "▶" : "⏸";
});

socket.on("log", d => addLog(d.msg, d.cls));
socket.on("frame", d => { $("liveFrame").src = "data:image/png;base64,"+d.image; $("videoOverlay").textContent = d.title||d.url; });

socket.on("bugs", bugs => {
  const list = $("bugList");
  if (!bugs||!bugs.length) { list.innerHTML = '<div style="color:var(--dim);font-size:12px">no bugs found</div>'; return; }
  list.innerHTML = bugs.map(b => '<div class="bug-item"><span class="cat">['+b.category+']</span><span class="url">'+b.page_url+'</span></div>').join("");
});

socket.on("captcha_needed", d => { $("captchaScreenshot").src = "data:image/png;base64,"+d.screenshot; openModal("captchaModal"); });

socket.on("crawl_complete", () => {
  addLog("crawl complete", "log-done");
  $("startBtn").style.display = "";
  $("pauseBtn").style.display = "none";
  $("stopBtn").style.display = "none";
  $("videoOverlay").textContent = "done";
});

socket.on("csv_export", csv => {
  const blob = new Blob([csv], {type:"text/csv"});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "pandora_bugs_"+Date.now()+".csv";
  a.click();
  URL.revokeObjectURL(a.href);
  addLog("CSV exported", "log-done");
});

socket.on("credentials", creds => {
  const list = $("credsList");
  if (!creds||!creds.length) { list.innerHTML = '<div style="color:var(--dim)">none saved</div>'; return; }
  list.innerHTML = creds.map(c => '<div style="display:flex;justify-content:space-between;align-items:center;padding:4px 0;border-bottom:1px solid var(--border)"><span><strong>'+c.label+'</strong> <span style="color:var(--dim);font-size:11px">'+(c.page_url||'all')+'</span></span><button class="btn-danger btn-sm" onclick="deleteCred('+c.id+')">✕</button></div>').join("");
});

function addLog(m,c) {
  const b=$("logBox");
  const d=document.createElement("div"); d.className=c; d.textContent=m;
  b.appendChild(d); b.scrollTop=b.scrollHeight;
}

function openModal(id){document.getElementById(id).classList.add("open")}
function closeModal(id){document.getElementById(id).classList.remove("open")}

function startCrawl() {
  let url = $("urlInput").value.trim();
  if (!url) return;
  if (!url.startsWith("http")) url = "https://"+url;
  $("urlInput").value = url;
  $("startBtn").style.display = "none";
  $("pauseBtn").style.display = "";
  $("stopBtn").style.display = "";
  $("liveFrame").src = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=";
  $("videoOverlay").textContent = "starting...";
  addLog("starting: "+url, "log-start");
  socket.emit("start", {url, headless:true, max_pages:+$("setMaxPages").value||500, max_depth:+$("setMaxDepth").value||10, concurrency:+$("setConcurrency").value||5, follow_subdomains:$("setFollowSub").value==="1", timeout:+$("setTimeout").value||300});
}

function stopCrawl(){socket.emit("stop",{}); $("startBtn").style.display=""; $("pauseBtn").style.display="none"; $("stopBtn").style.display="none";}
function togglePause(){socket.emit($("pauseBtn").textContent==="▶"?"resume":"pause",{});}
function exportCSV(){socket.emit("serve_bugs_csv",{});}
function resumeCaptcha(){socket.emit("captcha_done",{}); closeModal("captchaModal");}
function saveCred(){socket.emit("save_credential",{label:$("credLabel").value,page_url:$("credUrl").value,login_selector:$("credLoginSel").value,username_field:$("credUserField").value,password_field:$("credPassField").value,username_val:$("credUserVal").value,password_val:$("credPassVal").value});["credLabel","credUrl","credLoginSel","credUserField","credPassField","credUserVal","credPassVal"].forEach(id=>document.getElementById(id).value="");}
function deleteCred(id){socket.emit("delete_credential",{id});}

document.getElementById("startBtn").onclick=startCrawl;
document.getElementById("pauseBtn").onclick=togglePause;
document.getElementById("stopBtn").onclick=stopCrawl;
document.getElementById("csvBtn").onclick=exportCSV;
document.getElementById("credsBtn").onclick=()=>openModal("credsModal");
document.getElementById("settingsBtn").onclick=()=>openModal("settingsModal");
document.getElementById("credsCloseBtn").onclick=()=>closeModal("credsModal");
document.getElementById("settingsCloseBtn").onclick=()=>closeModal("settingsModal");
document.getElementById("credsSaveBtn").onclick=saveCred;
document.getElementById("captchaResumeBtn").onclick=resumeCaptcha;
document.querySelectorAll(".tab").forEach(t=>t.onclick=()=>{document.querySelectorAll(".tab,.tab-content").forEach(e=>e.classList.remove("active"));t.classList.add("active");document.getElementById("tab"+t.dataset.tab.charAt(0).toUpperCase()+t.dataset.tab.slice(1)).classList.add("active");});
document.getElementById("urlInput").onkeydown=e=>{if(e.key==="Enter")startCrawl()};

socket.on("disconnect", () => addLog("disconnected", "log-warn"));
</script>
</body>
</html>"""

# ── entry ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 7860))
    uvicorn.run("main:sio_asgi", host="0.0.0.0", port=port)
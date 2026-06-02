"""
streamedtom3u — HLS proxy for streamed.pk that produces a Jellyfin-compatible M3U.

How it works:
- /playlist.m3u             → generates a single M3U listing all live matches (multiple sources/streams)
- /stream/{src}/{id}/{n}.m3u8 → opens the upstream embed page in a headless browser, sniffs the
                                m3u8 response body, rewrites the relative TS paths to local /seg URLs,
                                and serves the rewritten playlist. The browser tab is kept alive so
                                live-stream m3u8 refreshes are captured automatically.
- /seg?u=...                → proxies a TS segment from the CDN with the required Referer header.

The upstream returns HLS playlists with a short token in the URL path that is "single-use" for the
m3u8 itself, but the TS segments only require a Referer header. So we keep a Playwright tab open per
active stream and grab the latest m3u8 body from the network log.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import re
import time
import urllib.parse
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, Response, StreamingResponse
from playwright.async_api import Browser, BrowserContext, async_playwright

log = logging.getLogger("streamedtom3u")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

API_BASE = "https://streamed.pk/api"
EMBED_BASE = "https://embedsports.top"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# How long a stream tab may sit idle (no client requests) before we close it
IDLE_CLOSE_SECONDS = 90
# How long we serve a cached m3u8 body before requiring a fresh sniff
M3U8_MAX_AGE_SECONDS = 8

STARTED_AT = time.time()

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data" if Path("/data").exists() else "."))
MANUAL_FILE = DATA_DIR / "manual.json"


@dataclass
class Stats:
    m3u8_served: int = 0
    m3u8_errors: int = 0
    segments_served: int = 0
    segment_errors: int = 0
    bytes_proxied: int = 0


stats = Stats()


class ManualSelection:
    """User-pinned matches that should appear in the playlist regardless of league."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        # Map echo_id -> {"title": str, "added_at": float}
        self._items: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        try:
            self._items = json.loads(self.path.read_text())
            if not isinstance(self._items, dict):
                self._items = {}
        except FileNotFoundError:
            self._items = {}
        except Exception as e:
            log.warning("manual selection load failed (%s): %s", self.path, e)
            self._items = {}

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._items, indent=2))
            tmp.replace(self.path)
        except Exception as e:
            log.warning("manual selection save failed (%s): %s", self.path, e)

    def all(self) -> dict[str, dict]:
        return dict(self._items)

    def contains(self, echo_id: str) -> bool:
        return echo_id in self._items

    async def add(self, echo_id: str, title: Optional[str] = None) -> None:
        async with self._lock:
            self._items[echo_id] = {"title": title, "added_at": time.time()}
            self._save()

    async def remove(self, echo_id: str) -> bool:
        async with self._lock:
            existed = self._items.pop(echo_id, None) is not None
            if existed:
                self._save()
            return existed


manual = ManualSelection(MANUAL_FILE)


@dataclass
class StreamTab:
    """One open headless-browser tab for one (source, id, streamNo)."""
    key: str
    embed_url: str
    context: BrowserContext
    page: object  # playwright.async_api.Page
    last_m3u8_body: Optional[str] = None
    last_m3u8_url: Optional[str] = None
    last_m3u8_at: float = 0.0
    last_accessed: float = field(default_factory=time.time)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: bool = False

    def touch(self) -> None:
        self.last_accessed = time.time()


class StreamRegistry:
    def __init__(self) -> None:
        self.tabs: dict[str, StreamTab] = {}
        self.browser: Optional[Browser] = None
        self.pw = None
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        asyncio.create_task(self._janitor())

    async def stop(self) -> None:
        for tab in list(self.tabs.values()):
            await self._close_tab(tab)
        if self.browser:
            await self.browser.close()
        if self.pw:
            await self.pw.stop()

    async def _janitor(self) -> None:
        while True:
            await asyncio.sleep(15)
            now = time.time()
            for key, tab in list(self.tabs.items()):
                if now - tab.last_accessed > IDLE_CLOSE_SECONDS:
                    log.info("closing idle tab %s", key)
                    await self._close_tab(tab)

    async def _close_tab(self, tab: StreamTab) -> None:
        tab.closed = True
        self.tabs.pop(tab.key, None)
        try:
            await tab.context.close()
        except Exception:
            pass

    async def get_or_open(self, source: str, mid: str, stream_no: int) -> StreamTab:
        key = f"{source}/{mid}/{stream_no}"
        async with self.lock:
            tab = self.tabs.get(key)
            if tab and not tab.closed:
                tab.touch()
                return tab

            embed_url = f"{EMBED_BASE}/embed/{source}/{mid}/{stream_no}"
            log.info("opening tab %s -> %s", key, embed_url)
            assert self.browser is not None
            ctx = await self.browser.new_context(user_agent=UA)
            page = await ctx.new_page()
            tab = StreamTab(key=key, embed_url=embed_url, context=ctx, page=page)
            self.tabs[key] = tab

            async def on_response(resp):  # type: ignore[no-untyped-def]
                url = resp.url
                if ".m3u8" not in url:
                    return
                try:
                    body = await resp.text()
                except Exception:
                    return
                # Only accept live playlists (must contain at least one segment)
                if "#EXTINF" not in body:
                    return
                tab.last_m3u8_body = body
                tab.last_m3u8_url = url
                tab.last_m3u8_at = time.time()
                log.debug("captured m3u8 for %s (%d bytes)", key, len(body))

            page.on("response", lambda r: asyncio.create_task(on_response(r)))

            try:
                await page.goto(embed_url, wait_until="domcontentloaded", timeout=30000)
            except Exception as e:
                log.warning("goto failed for %s: %s", key, e)

            # Wait briefly for the first m3u8 to arrive
            for _ in range(50):
                if tab.last_m3u8_body:
                    break
                await asyncio.sleep(0.2)

            return tab

    async def fresh_m3u8(self, tab: StreamTab) -> tuple[str, str]:
        async with tab.lock:
            now = time.time()
            if not tab.last_m3u8_body or now - tab.last_m3u8_at > M3U8_MAX_AGE_SECONDS:
                # Force the player to fetch a fresh playlist by reloading the embed.
                # The player polls the m3u8 itself; reload guarantees a hit even if it stopped.
                try:
                    await tab.page.reload(wait_until="domcontentloaded", timeout=15000)
                except Exception as e:
                    log.warning("reload failed for %s: %s", tab.key, e)
                # wait up to 8s for a new m3u8
                deadline = time.time() + 8
                while time.time() < deadline:
                    if tab.last_m3u8_at > now:
                        break
                    await asyncio.sleep(0.2)
            if not tab.last_m3u8_body or not tab.last_m3u8_url:
                raise HTTPException(502, "Could not capture m3u8 from upstream")
            return tab.last_m3u8_body, tab.last_m3u8_url


registry = StreamRegistry()


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[no-untyped-def]
    await registry.start()
    yield
    await registry.stop()


app = FastAPI(title="streamedtom3u", lifespan=lifespan)


# ---------- Helpers ----------

def _b64url_encode(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")


def _b64url_decode(s: str) -> str:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad).decode()


def _resolve(base: str, ref: str) -> str:
    """Resolve a relative segment URI against the m3u8 base URL (HLS semantics)."""
    return urllib.parse.urljoin(base, ref)


def _rewrite_m3u8(body: str, m3u8_url: str, request_base: str) -> str:
    """Rewrite all segment URIs and nested playlist URIs to go through our /seg proxy."""
    out_lines: list[str] = []
    for line in body.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            # Some tags (EXT-X-KEY, EXT-X-MAP) carry URI="..."; rewrite those too.
            if 'URI="' in s:
                def repl(m: re.Match) -> str:
                    abs_url = _resolve(m3u8_url, m.group(1))
                    return f'URI="{request_base}/seg?u={_b64url_encode(abs_url)}"'
                line = re.sub(r'URI="([^"]+)"', repl, line)
            out_lines.append(line)
            continue
        abs_url = _resolve(m3u8_url, s)
        out_lines.append(f"{request_base}/seg?u={_b64url_encode(abs_url)}")
    return "\n".join(out_lines) + "\n"


def _request_base(request: Request) -> str:
    # Honor reverse-proxy headers if present
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.netloc)
    return f"{scheme}://{host}"


# ---------- Routes ----------

@app.get("/")
async def index() -> dict:
    return {
        "name": "streamedtom3u",
        "endpoints": {
            "playlist": "/playlist.m3u",
            "stream": "/stream/{source}/{id}/{streamNo}.m3u8",
            "segment": "/seg?u=...",
        },
        "active_tabs": list(registry.tabs.keys()),
    }


# League detection from delta/admin source IDs (echo IDs don't encode the league).
# Order matters: longer / more specific substrings must come first.
LEAGUE_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("2. Bundesliga", ("2-bundesliga", "bundesliga-2", "2nd-bundesliga", "zweite-bundesliga")),
    ("3. Liga",        ("3-liga", "3rd-liga", "dritte-liga", "liga-3")),
    ("Bundesliga",     ("germany-bundesliga", "1-bundesliga", "bundesliga")),
    ("WM",             ("fifa-world-cup", "world-cup", "weltmeisterschaft", "fifa-wm")),
    ("EM",             ("uefa-european-championship", "european-championship",
                        "uefa-euro", "euro-2024", "euro-2028", "europameisterschaft")),
]


def detect_league(match: dict) -> Optional[str]:
    haystack = (match.get("id") or "").lower()
    for s in match.get("sources") or []:
        haystack += " " + (s.get("id") or "").lower()
    for label, needles in LEAGUE_RULES:
        if any(n in haystack for n in needles):
            return label
    return None


def first_echo_source(match: dict) -> Optional[dict]:
    for s in match.get("sources") or []:
        if s.get("source") == "echo" and s.get("id"):
            return s
    return None


async def _fetch_match_pool(scope: str) -> list[dict]:
    """Get the candidate pool of football matches for the chosen scope."""
    if scope == "live":
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{API_BASE}/matches/live")
            r.raise_for_status()
            return [m for m in r.json() if m.get("category") == "football"]
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{API_BASE}/matches/football")
        r.raise_for_status()
        return r.json()


def _playlist_entry_for(m: dict, group_label: str) -> Optional[tuple[str, str, str]]:
    """Return (tvg_id, extinf, stream_path_suffix) for a match, or None if no echo source."""
    echo = first_echo_source(m)
    if not echo:
        return None
    mid = m.get("id")
    title = m.get("title", mid)
    poster = m.get("poster") or ""
    logo = f"https://streamed.pk{poster}" if poster.startswith("/") else (poster or "")
    source = "echo"
    sid = echo["id"]
    stream_no = 1
    tvg_id = f"{source}-{sid}-{stream_no}"
    extinf = (
        f'#EXTINF:-1 tvg-id="{tvg_id}" tvg-name="{title}" '
        f'tvg-logo="{logo}" group-title="{group_label}",{title}'
    )
    return tvg_id, extinf, f"/stream/{source}/{sid}/{stream_no}.m3u8"


@app.get("/playlist.m3u")
async def playlist(request: Request, scope: str = "today") -> PlainTextResponse:
    """
    Generate the M3U:
      - all matches whose league is detected as Bundesliga/2. BL/3. Liga/WM/EM
      - PLUS any match the user manually pinned via the dashboard (group = "Andere")

    Only the first `echo` stream per match is included.

    - scope=today (default): today's football matches (live + upcoming)
    - scope=live:            only currently-live football matches
    """
    matches = await _fetch_match_pool(scope)
    base = _request_base(request)
    lines = ["#EXTM3U"]
    seen_keys: set[str] = set()

    for m in matches:
        echo = first_echo_source(m)
        if not echo:
            continue
        echo_id = echo["id"]
        league = detect_league(m)
        is_manual = manual.contains(echo_id)
        if not league and not is_manual:
            continue

        group_label = league if league else "Andere"
        entry = _playlist_entry_for(m, group_label)
        if not entry:
            continue
        tvg_id, extinf, suffix = entry
        if tvg_id in seen_keys:
            continue
        seen_keys.add(tvg_id)
        lines.append(extinf)
        lines.append(f"{base}{suffix}")
    body = "\n".join(lines) + "\n"
    return PlainTextResponse(body, media_type="application/vnd.apple.mpegurl")


@app.get("/debug/football")
async def debug_football(scope: str = "today") -> list[dict]:
    """Diagnostic: show every football match the API returns, the detected league,
    and whether an echo stream exists. Useful when the playlist looks empty."""
    path = "/matches/live" if scope == "live" else "/matches/football"
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{API_BASE}{path}")
        r.raise_for_status()
        raw = r.json()
    matches = [m for m in raw if m.get("category") == "football"] if scope == "live" else raw
    out = []
    for m in matches:
        out.append({
            "title": m.get("title"),
            "league": detect_league(m),
            "echo": (first_echo_source(m) or {}).get("id"),
            "sources": [(s.get("source"), s.get("id")) for s in m.get("sources") or []],
        })
    return out


@app.get("/streams/{source}/{mid}")
async def list_streams(source: str, mid: str) -> list[dict]:
    """Helper: list all available streams for one match (so you can add streamNo 2+ if you want)."""
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"{API_BASE}/stream/{source}/{mid}")
        r.raise_for_status()
        return r.json()


@app.get("/stream/{source}/{mid}/{stream_no}.m3u8")
async def stream_m3u8(source: str, mid: str, stream_no: int, request: Request) -> PlainTextResponse:
    try:
        tab = await registry.get_or_open(source, mid, stream_no)
        body, m3u8_url = await registry.fresh_m3u8(tab)
    except Exception:
        stats.m3u8_errors += 1
        raise
    rewritten = _rewrite_m3u8(body, m3u8_url, _request_base(request))
    stats.m3u8_served += 1
    return PlainTextResponse(rewritten, media_type="application/vnd.apple.mpegurl")


@app.get("/seg")
async def segment(u: str) -> StreamingResponse:
    try:
        target = _b64url_decode(u)
    except Exception:
        raise HTTPException(400, "bad u")
    if not target.startswith("https://") and not target.startswith("http://"):
        raise HTTPException(400, "bad scheme")

    headers = {
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Encoding": "identity",  # avoid re-compressing TS
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": EMBED_BASE,
        "Referer": f"{EMBED_BASE}/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
    }

    client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=60.0))
    upstream = await client.send(client.build_request("GET", target, headers=headers), stream=True)

    if upstream.status_code >= 400:
        body = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        stats.segment_errors += 1
        raise HTTPException(upstream.status_code, body.decode(errors="replace")[:200])

    media_type = upstream.headers.get("content-type", "video/mp2t")
    stats.segments_served += 1

    async def gen():
        try:
            async for chunk in upstream.aiter_bytes(64 * 1024):
                stats.bytes_proxied += len(chunk)
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    # Strip hop-by-hop headers from upstream response
    fwd = {}
    for k in ("content-length", "cache-control", "etag", "last-modified"):
        if k in upstream.headers:
            fwd[k] = upstream.headers[k]

    return StreamingResponse(gen(), media_type=media_type, headers=fwd)


# ---------- UI app (separate FastAPI on a separate port) ----------

ui_app = FastAPI(title="streamedtom3u-ui")


@ui_app.get("/api/status")
async def ui_status() -> dict:
    return {
        "uptime_seconds": int(time.time() - STARTED_AT),
        "browser_ok": bool(registry.browser and registry.browser.is_connected()),
        "active_tabs": len(registry.tabs),
        "m3u8_served": stats.m3u8_served,
        "m3u8_errors": stats.m3u8_errors,
        "segments_served": stats.segments_served,
        "segment_errors": stats.segment_errors,
        "bytes_proxied": stats.bytes_proxied,
        "idle_close_seconds": IDLE_CLOSE_SECONDS,
        "m3u8_max_age_seconds": M3U8_MAX_AGE_SECONDS,
    }


@ui_app.get("/api/streams")
async def ui_streams() -> list[dict]:
    now = time.time()
    out = []
    for tab in registry.tabs.values():
        out.append({
            "key": tab.key,
            "embed_url": tab.embed_url,
            "m3u8_url": tab.last_m3u8_url,
            "m3u8_bytes": len(tab.last_m3u8_body) if tab.last_m3u8_body else 0,
            "m3u8_age_seconds": round(now - tab.last_m3u8_at, 1) if tab.last_m3u8_at else None,
            "idle_seconds": round(now - tab.last_accessed, 1),
            "closed": tab.closed,
        })
    out.sort(key=lambda x: x["idle_seconds"])
    return out


@ui_app.delete("/api/streams/{source}/{mid}/{stream_no}")
async def ui_close_stream(source: str, mid: str, stream_no: int) -> dict:
    key = f"{source}/{mid}/{stream_no}"
    tab = registry.tabs.get(key)
    if not tab:
        raise HTTPException(404, "not found")
    await registry._close_tab(tab)
    return {"closed": key}


@ui_app.get("/api/matches")
async def ui_matches(scope: str = "today") -> list[dict]:
    """All today's football matches with league detection + manual-pin state."""
    matches = await _fetch_match_pool(scope)
    now_ms = time.time() * 1000
    out = []
    for m in matches:
        league = detect_league(m)
        echo = first_echo_source(m)
        echo_id = echo["id"] if echo else None
        is_manual = bool(echo_id and manual.contains(echo_id))
        out.append({
            "title": m.get("title"),
            "league": league,
            "echo_id": echo_id,
            "is_manual": is_manual,
            "in_playlist": bool(echo_id and (league or is_manual)),
            "starts_in_minutes": round((m.get("date", 0) - now_ms) / 60000) if m.get("date") else None,
        })
    return out


@ui_app.get("/api/manual")
async def ui_manual_list() -> list[dict]:
    return [{"echo_id": k, **v} for k, v in manual.all().items()]


@ui_app.post("/api/manual/{echo_id}")
async def ui_manual_add(echo_id: str, title: Optional[str] = None) -> dict:
    if not echo_id:
        raise HTTPException(400, "echo_id required")
    await manual.add(echo_id, title)
    return {"added": echo_id}


@ui_app.delete("/api/manual/{echo_id}")
async def ui_manual_remove(echo_id: str) -> dict:
    removed = await manual.remove(echo_id)
    if not removed:
        raise HTTPException(404, "not pinned")
    return {"removed": echo_id}


INDEX_HTML = """\
<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>streamedtom3u · Dashboard</title>
<style>
  :root {
    --bg: #0f1117; --panel: #181b25; --border: #262a38;
    --fg: #e6e8ee; --muted: #8b91a6; --accent: #6ea8fe;
    --ok: #4ade80; --warn: #fbbf24; --err: #f87171;
    --mono: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 system-ui, -apple-system, sans-serif;
    background: var(--bg); color: var(--fg); }
  header { padding: 20px 28px; border-bottom: 1px solid var(--border);
    display: flex; align-items: baseline; gap: 16px; }
  header h1 { margin: 0; font-size: 18px; font-weight: 600; }
  header .sub { color: var(--muted); font-size: 12px; }
  header .pill { margin-left: auto; padding: 4px 10px; border-radius: 999px;
    font-size: 11px; font-weight: 600; letter-spacing: .03em; }
  .pill.ok { background: rgba(74,222,128,.15); color: var(--ok); }
  .pill.err { background: rgba(248,113,113,.15); color: var(--err); }
  main { padding: 24px 28px; display: grid; gap: 24px;
    grid-template-columns: repeat(auto-fit, minmax(380px, 1fr)); }
  .card { background: var(--panel); border: 1px solid var(--border);
    border-radius: 10px; overflow: hidden; }
  .card h2 { margin: 0; padding: 14px 18px; font-size: 13px; font-weight: 600;
    color: var(--muted); text-transform: uppercase; letter-spacing: .05em;
    border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 8px; }
  .card .body { padding: 16px 18px; }
  .stats { display: grid; grid-template-columns: repeat(2,1fr); gap: 14px 24px; }
  .stat .k { color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .04em; }
  .stat .v { font-family: var(--mono); font-size: 18px; font-weight: 500; margin-top: 2px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 9px 18px; text-align: left; border-bottom: 1px solid var(--border); }
  th { color: var(--muted); font-weight: 500; font-size: 11px;
    text-transform: uppercase; letter-spacing: .04em; background: rgba(0,0,0,.15); }
  td.mono { font-family: var(--mono); font-size: 12px; color: var(--muted); }
  td a { color: var(--accent); text-decoration: none; }
  td a:hover { text-decoration: underline; }
  td .badge { padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 600;
    background: rgba(110,168,254,.15); color: var(--accent); }
  td .badge.muted { background: rgba(139,145,166,.15); color: var(--muted); }
  td .badge.warn { background: rgba(251,191,36,.15); color: var(--warn); }
  td button { background: transparent; border: 1px solid var(--border); color: var(--err);
    border-radius: 6px; padding: 3px 10px; cursor: pointer; font-size: 11px; }
  td button:hover { border-color: var(--err); }
  td button.pin { color: var(--muted); font-size: 14px; padding: 2px 8px; line-height: 1; }
  td button.pin:hover { border-color: var(--accent); color: var(--accent); }
  td button.pin.on { color: var(--warn); border-color: rgba(251,191,36,.4); }
  .filter { display: flex; gap: 6px; margin-left: auto; }
  .filter button { background: transparent; border: 1px solid var(--border);
    color: var(--muted); border-radius: 6px; padding: 4px 10px; cursor: pointer;
    font-size: 11px; font-weight: 500; }
  .filter button.active { background: var(--accent); color: #0f1117; border-color: var(--accent); }
  .empty { padding: 22px 18px; color: var(--muted); font-style: italic; text-align: center; }
  footer { padding: 16px 28px; color: var(--muted); font-size: 11px; }
  footer code { font-family: var(--mono); background: var(--panel);
    padding: 2px 6px; border-radius: 4px; }
</style>
</head>
<body>
<header>
  <h1>streamedtom3u</h1>
  <span class="sub" id="sub">…</span>
  <span class="pill" id="health">…</span>
</header>

<main>
  <section class="card">
    <h2>Status</h2>
    <div class="body">
      <div class="stats" id="stats">…</div>
    </div>
  </section>

  <section class="card" style="grid-column: 1 / -1;">
    <h2>Aktive Streams <span id="tab-count" style="color:var(--muted);font-weight:400;"></span></h2>
    <div id="streams-wrap"><div class="empty">Lade …</div></div>
  </section>

  <section class="card" style="grid-column: 1 / -1;">
    <h2>
      Spiele (heute) <span id="match-count" style="color:var(--muted);font-weight:400;"></span>
      <span class="filter" id="match-filter">
        <button data-filter="all" class="active">alle</button>
        <button data-filter="playlist">in Playlist</button>
        <button data-filter="pinned">★ angeheftet</button>
      </span>
    </h2>
    <div id="matches-wrap"><div class="empty">Lade …</div></div>
  </section>
</main>

<footer>
  Refresh: 5 s · Playlist: <code id="playlist-url">…</code>
</footer>

<script>
const fmtBytes = (n) => {
  if (!n) return "0 B";
  const u = ["B","KB","MB","GB","TB"]; let i = 0;
  while (n >= 1024 && i < u.length-1) { n /= 1024; i++; }
  return n.toFixed(i === 0 ? 0 : 1) + " " + u[i];
};
const fmtDuration = (s) => {
  if (s == null) return "—";
  s = Math.round(s);
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s/60) + "m " + (s%60) + "s";
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60);
  return h + "h " + m + "m";
};

function escape(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"
  })[c]);
}

async function load() {
  // Default API host: current origin; the stream URLs in the table point to the
  // proxy port (one below the UI port — convention: UI=8768, proxy=8765 by default).
  const playlistBase = window.location.protocol + "//" + window.location.hostname + ":" + PROXY_PORT;
  document.getElementById("playlist-url").textContent = playlistBase + "/playlist.m3u";

  try {
    const [statusRes, streamsRes, matchesRes] = await Promise.all([
      fetch("/api/status"),
      fetch("/api/streams"),
      fetch("/api/matches"),
    ]);
    const status = await statusRes.json();
    const streams = await streamsRes.json();
    const matches = await matchesRes.json();

    document.getElementById("sub").textContent =
      "Uptime " + fmtDuration(status.uptime_seconds);
    const ok = status.browser_ok;
    const pill = document.getElementById("health");
    pill.className = "pill " + (ok ? "ok" : "err");
    pill.textContent = ok ? "● Browser läuft" : "● Browser tot";

    document.getElementById("stats").innerHTML = `
      <div class="stat"><div class="k">Aktive Tabs</div><div class="v">${status.active_tabs}</div></div>
      <div class="stat"><div class="k">Uptime</div><div class="v">${fmtDuration(status.uptime_seconds)}</div></div>
      <div class="stat"><div class="k">m3u8 ausgeliefert</div><div class="v">${status.m3u8_served}</div></div>
      <div class="stat"><div class="k">m3u8 Fehler</div><div class="v" style="color:${status.m3u8_errors?'var(--err)':'inherit'}">${status.m3u8_errors}</div></div>
      <div class="stat"><div class="k">TS-Segmente</div><div class="v">${status.segments_served}</div></div>
      <div class="stat"><div class="k">TS-Fehler</div><div class="v" style="color:${status.segment_errors?'var(--err)':'inherit'}">${status.segment_errors}</div></div>
      <div class="stat"><div class="k">Daten gestreamt</div><div class="v">${fmtBytes(status.bytes_proxied)}</div></div>
      <div class="stat"><div class="k">Idle-Limit</div><div class="v">${status.idle_close_seconds}s</div></div>
    `;

    document.getElementById("tab-count").textContent = `(${streams.length})`;
    const streamsWrap = document.getElementById("streams-wrap");
    if (streams.length === 0) {
      streamsWrap.innerHTML = '<div class="empty">Kein aktiver Stream — Tabs werden geöffnet, sobald ein Client einen Stream anfordert.</div>';
    } else {
      streamsWrap.innerHTML = `
        <table>
          <thead><tr>
            <th>Match</th><th>m3u8-Alter</th><th>Größe</th><th>Idle</th><th>Embed</th><th></th>
          </tr></thead>
          <tbody>
            ${streams.map(s => `
              <tr>
                <td class="mono">${escape(s.key)}</td>
                <td>${s.m3u8_age_seconds != null
                    ? '<span class="badge' + (s.m3u8_age_seconds > 30 ? ' muted' : '') + '">' + fmtDuration(s.m3u8_age_seconds) + '</span>'
                    : '<span class="badge muted">—</span>'}</td>
                <td>${fmtBytes(s.m3u8_bytes)}</td>
                <td>${fmtDuration(s.idle_seconds)}</td>
                <td class="mono"><a href="${escape(s.embed_url)}" target="_blank" rel="noopener">open ↗</a></td>
                <td><button data-key="${escape(s.key)}">stop</button></td>
              </tr>
            `).join("")}
          </tbody>
        </table>`;
      streamsWrap.querySelectorAll("button[data-key]").forEach(b => {
        b.onclick = async () => {
          if (!confirm("Tab schließen: " + b.dataset.key + "?")) return;
          b.disabled = true;
          await fetch("/api/streams/" + b.dataset.key, { method: "DELETE" });
          await load();
        };
      });
    }

    renderMatches(matches, playlistBase);

  } catch (err) {
    document.getElementById("health").className = "pill err";
    document.getElementById("health").textContent = "● Fehler beim Laden";
    console.error(err);
  }
}

let currentFilter = "all";
let lastMatches = [];
let lastPlaylistBase = "";

function renderMatches(matches, playlistBase) {
  lastMatches = matches;
  lastPlaylistBase = playlistBase;

  const inPlaylist = matches.filter(m => m.in_playlist);
  const pinned = matches.filter(m => m.is_manual);
  document.getElementById("match-count").textContent =
    `(${inPlaylist.length} in Playlist · ${pinned.length} angeheftet · ${matches.length} gesamt)`;

  const filtered = matches.filter(m => {
    if (currentFilter === "playlist") return m.in_playlist;
    if (currentFilter === "pinned") return m.is_manual;
    return true;
  });

  const wrap = document.getElementById("matches-wrap");
  if (filtered.length === 0) {
    wrap.innerHTML = '<div class="empty">Keine Spiele in dieser Ansicht.</div>';
    return;
  }
  wrap.innerHTML = `
    <table>
      <thead><tr>
        <th style="width:1px"></th><th>Match</th><th>Gruppe</th>
        <th>Startet</th><th>echo-ID</th><th>Play</th>
      </tr></thead>
      <tbody>
        ${filtered.map(m => {
          const canPin = !!m.echo_id;
          const playUrl = m.in_playlist
            ? playlistBase + "/stream/echo/" + encodeURIComponent(m.echo_id) + "/1.m3u8"
            : null;
          const group = m.league
            ? '<span class="badge">' + escape(m.league) + '</span>'
            : m.is_manual
              ? '<span class="badge warn">★ Andere</span>'
              : '<span class="badge muted">—</span>';
          return `
            <tr style="${m.in_playlist ? '' : 'opacity:.55'}">
              <td>${canPin
                ? '<button class="pin' + (m.is_manual ? ' on' : '') + '" '
                  + 'data-echo="' + escape(m.echo_id) + '" '
                  + 'data-title="' + escape(m.title || '') + '" '
                  + 'data-on="' + (m.is_manual ? '1' : '0') + '" '
                  + 'title="' + (m.is_manual ? 'Aus der Playlist entfernen' : 'Zur Playlist hinzufügen') + '">★</button>'
                : ''}</td>
              <td>${escape(m.title)}</td>
              <td>${group}</td>
              <td>${m.starts_in_minutes == null ? "—" :
                   m.starts_in_minutes <= 0 ? '<span class="badge">live</span>' :
                   'in ' + m.starts_in_minutes + 'm'}</td>
              <td class="mono">${escape(m.echo_id || "—")}</td>
              <td>${playUrl ? '<a href="' + playUrl + '" target="_blank" rel="noopener">m3u8 ↗</a>' : ""}</td>
            </tr>`;
        }).join("")}
      </tbody>
    </table>`;

  wrap.querySelectorAll("button.pin").forEach(b => {
    b.onclick = async () => {
      const echoId = b.dataset.echo;
      const isOn = b.dataset.on === "1";
      b.disabled = true;
      const method = isOn ? "DELETE" : "POST";
      const url = "/api/manual/" + encodeURIComponent(echoId)
                + (isOn ? "" : "?title=" + encodeURIComponent(b.dataset.title || ""));
      const res = await fetch(url, { method });
      if (!res.ok && res.status !== 404) {
        alert("Fehler: " + res.status);
        b.disabled = false;
        return;
      }
      await load();
    };
  });
}

document.getElementById("match-filter").addEventListener("click", (e) => {
  if (e.target.tagName !== "BUTTON") return;
  currentFilter = e.target.dataset.filter;
  document.querySelectorAll("#match-filter button").forEach(b =>
    b.classList.toggle("active", b === e.target));
  if (lastMatches.length) renderMatches(lastMatches, lastPlaylistBase);
});

const PROXY_PORT = __PROXY_PORT__;
load();
setInterval(load, 5000);
</script>
</body>
</html>
"""


@ui_app.get("/", response_class=Response)
async def ui_index() -> Response:
    proxy_port = int(os.environ.get("PORT", "8765"))
    html = INDEX_HTML.replace("__PROXY_PORT__", str(proxy_port))
    return Response(html, media_type="text/html; charset=utf-8")


# ---------- Entrypoint: run both servers in one process ----------

async def _serve_both() -> None:
    import uvicorn

    proxy_port = int(os.environ.get("PORT", "8765"))
    ui_port = int(os.environ.get("UI_PORT", "8768"))

    cfg_proxy = uvicorn.Config(app, host="0.0.0.0", port=proxy_port, log_level=os.environ.get("LOG_LEVEL", "info").lower())
    cfg_ui = uvicorn.Config(ui_app, host="0.0.0.0", port=ui_port, log_level="warning")

    server_proxy = uvicorn.Server(cfg_proxy)
    server_ui = uvicorn.Server(cfg_ui)

    log.info("proxy on :%d  ·  UI on :%d", proxy_port, ui_port)
    await asyncio.gather(server_proxy.serve(), server_ui.serve())


if __name__ == "__main__":
    asyncio.run(_serve_both())

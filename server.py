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
import logging
import os
import re
import time
import urllib.parse
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
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


@app.get("/playlist.m3u")
async def playlist(request: Request, scope: str = "today") -> PlainTextResponse:
    """
    German-football-only playlist (Bundesliga 1/2/3, WM, EM).
    Only the first `echo` stream per match is exposed (one entry per match).

    - scope=today (default): today's football matches (live + upcoming)
    - scope=live:            only currently-live football matches
    """
    if scope == "live":
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{API_BASE}/matches/live")
            r.raise_for_status()
            matches = [m for m in r.json() if m.get("category") == "football"]
    else:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(f"{API_BASE}/matches/football")
            r.raise_for_status()
            matches = r.json()

    base = _request_base(request)
    lines = ["#EXTM3U"]
    for m in matches:
        league = detect_league(m)
        if not league:
            continue
        echo = first_echo_source(m)
        if not echo:
            continue

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
            f'tvg-logo="{logo}" group-title="{league}",{title}'
        )
        lines.append(extinf)
        lines.append(f"{base}/stream/{source}/{sid}/{stream_no}.m3u8")
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
    tab = await registry.get_or_open(source, mid, stream_no)
    body, m3u8_url = await registry.fresh_m3u8(tab)
    rewritten = _rewrite_m3u8(body, m3u8_url, _request_base(request))
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
        raise HTTPException(upstream.status_code, body.decode(errors="replace")[:200])

    media_type = upstream.headers.get("content-type", "video/mp2t")

    async def gen():
        try:
            async for chunk in upstream.aiter_bytes(64 * 1024):
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8765")))

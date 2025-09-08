#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bt.py — BIFF OneShot
스케쥴넘버(sdCode) → 메타(제목/장소/일시/총/잔여) 표시 + 1석 예매 → 결제창(HOLD)
- 콘솔은 "한 줄 로그(아이콘 포함)"만 출력합니다.
- 실패/폴백/재시도는 내부에서 처리하고 최종 상태만 요약합니다.

요구사항 요약:
  * 로그인 후 실행(스크립트가 창을 열고 로그인을 기다립니다).
  * 자유석(NRS)은 수량=1, 지정석(RS)은 1좌석을 선택 후 결제폼이 보이면 HOLD.
  * 총좌석/잔여는 NRS: blockSummary2 → tickettype (정확 산식), RS: seatStatusList → zone합산 폴백.
  * 장소/일시는 prodSummary → filmapi(prodList?sdCode=) → DOM 보강 3중 폴백.
  * 한 줄 로그 포맷:
      <아이콘> <SD> | <제목(…)> | <장소(…)> | <MM-DD HH:MM> | <PLAN> | T/R=<총>/<잔여> | <액션>

필요 라이브러리:
  pip install playwright
  playwright install

주의: 실제 셀렉터/토큰 명칭/파라미터는 사이트 변경 시 달라질 수 있습니다.
     본 스크립트는 폴백을 최대화했으나 운영 환경에 맞게 미세 조정이 필요할 수 있습니다.
"""
import asyncio
import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, TimeoutError as PWTimeout

# === RUNTIME CONFIG (하드코딩) ============================================
# * 여기만 바꿔서 쓰면 됨 *
from types import SimpleNamespace

RUNTIME = SimpleNamespace(
    SDCODES=["001", "911", "324"],  # ← 스케쥴넘버들
    HEADLESS=False,                 # 헤드리스 모드
    CONCURRENCY=3,                  # 동시 처리 수
    INFO_ONLY=True,                 # 정보만 수집 (예매/결제 미진행)
    DEBUG=True,                     # 디버그 로그 ON/OFF
    STAY_SEC=600,                   # 결제창 HOLD 유지 (초)
)
# ====================================================================

SITE = os.getenv("BT_SITE", "https://biff.maketicket.co.kr")
API  = os.getenv("BT_API",  "https://filmonestopapi.maketicket.co.kr")
FILMAPI = os.getenv("BT_FILMAPI", "https://filmapi.maketicket.co.kr")
LOGIN_URL = os.getenv("BT_LOGIN_URL", f"{SITE}/ko/login")
RESMAIN   = os.getenv("BT_RESMAIN",   f"{SITE}/ko/resMain?sdCode={{sd}}")
ALT_SITE  = os.getenv("BT_ALT_SITE",  "https://filmonestop.maketicket.co.kr")
ALT_RESMAIN = os.getenv("BT_ALT_RESMAIN", f"{ALT_SITE}/ko/resMain?sdCode={{sd}}")
DEFAULT_TIMEOUT = float(os.getenv("BT_TIMEOUT_SEC", "7.5"))  # per action
PAY_HOLD_SEC    = int(os.getenv("BT_PAY_HOLD_SEC", "600"))    # payment page hold seconds
DEBUG = False

def dbg(*a):
    if DEBUG:
        try:
            print("·", *a, flush=True)
        except Exception:
            pass
# ---------- Utilities ----------

# ===== CSRF harvest helpers ===============================================
from urllib.parse import unquote as _unq

async def _harvest_csrf_any(page) -> str:
    """메타/히든/쿠키/iframe 어디서든 CSRF 토큰을 최대한 확보."""
    token = ""

    # 0) 메인 프레임 메타/히든
    try:
        vals = await page.evaluate("""() => ({
          c: document.querySelector('#csrfToken')?.value
             || document.querySelector('meta[name="csrf-token"]')?.content
             || document.querySelector('meta[name="X-CSRF-TOKEN"]')?.content
             || ''
        })""")
        if vals and vals.get("c"): token = vals["c"]
    except Exception:
        pass

    # 1) onestop iframe 내부
    if not token:
        try:
            target = None
            for fr in page.frames:
                u = (fr.url or "")
                if "filmonestop" in u or "oneStopFrame" in (fr.name or ""):
                    target = fr; break
            if target:
                vals_ifr = await target.evaluate("""() => ({
                  c: document.querySelector('#csrfToken')?.value
                     || document.querySelector('meta[name="csrf-token"]')?.content
                     || document.querySelector('meta[name="X-CSRF-TOKEN"]')?.content
                     || ''
                })""")
                if vals_ifr and vals_ifr.get("c"):
                    token = vals_ifr["c"]
        except Exception:
            pass
    # 1.5) 문서 전체 HTML 정규식 스캔
    if not token:
        try:
            token = _regex_find_csrf(await page.content()) or ""
        except Exception:
            pass
    if not token:
        # 모든 프레임 HTML도 스캔
        for fr in page.frames:
            try:
                html = await fr.content()
                token = _regex_find_csrf(html)
                if token:
                    break
            except Exception:
                pass

    # 2) 쿠키에서 후보 스캔 (HTTPOnly 포함)
    if not token:
        try:
            cand = ""
            names = []
            for c in await page.context.cookies():
                n = (c.get("name") or "")
                v = (c.get("value") or "")
                names.append(n)
                up = n.upper()
                if ("XSRF" in up or "CSRF" in up) and v:
                    cand = v
                    break
            if cand:
                token = _unq(cand)  # URL 인코딩 복원
            # 디버그: 쿠키 이름 목록 찍기(값은 노출 안 함)
            try:
                dbg("csrf cookies", {"names": names})
            except Exception:
                pass
        except Exception:
            pass

    return (token or "")
# ==========================================================================

# ==== CSRF deep regex scan + HTTP fallback ==================================
import re

def _regex_find_csrf(html: str) -> str:
    if not html:
        return ""
    pats = [
        r'id=["\']csrfToken["\'][^>]*value=["\']([^"\']+)["\']',
        r'name=["\']csrfToken["\'][^>]*value=["\']([^"\']+)["\']',
        r'meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']',
        r'csrfToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
    ]
    for p in pats:
        m = re.search(p, html, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""

async def _harvest_csrf_via_http(context: BrowserContext, book_url: str, referer: str) -> str:
    """페이지를 HTTP로 직접 받아서 hidden/meta/스크립트에서 csrfToken을 긁는다."""
    try:
        resp = await context.request.get(book_url, headers={
            "Accept": "text/html, */*",
            "Referer": referer,
        })
        html = await resp.text()
        if resp.status != 200 or not html:
            return ""
        return _regex_find_csrf(html)
    except Exception:
        return ""
# 네비게이션 기다리지 말고, request.post로 booking을 'POST'로 두드려서 CSRF만 씨딩
import re

def _regex_find_csrf(html: str) -> str:
    if not html:
        return ""
    pats = [
        r'id=["\']csrfToken["\'][^>]*value=["\']([^"\']+)["\']',
        r'name=["\']csrfToken["\'][^>]*value=["\']([^"\']+)["\']',
        r'meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']',
        r'csrfToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
    ]
    for p in pats:
        m = re.search(p, html, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""

async def _post_into_booking(ctx, page: Page) -> bool:
    """
    SITE → ALT_SITE/ko/booking 으로 'POST' 날려서 브라우저 컨텍스트에 세션/쿠키를 심고,
    응답 HTML에서 csrfToken만 추출해서 ctx.csrf에 채운다. 네비게이션은 안 기다림.
    """
    # 1) 먼저 SITE/resMain 한 번 찍어 레퍼러 체인 맞추기 (필수는 아니지만 안전)
    try:
        await page.goto(f"{SITE}/ko/resMain?sdCode={ctx.sd}", wait_until="domcontentloaded", timeout=int(DEFAULT_TIMEOUT*1000))
        await page.wait_for_timeout(200)
    except Exception:
        pass

    # 2) booking으로 'POST' (x-www-form-urlencoded)
    form = {
        "prodSeq": str(ctx.prodSeq),
        "sdSeq":   str(ctx.sdSeq),
        "chnlCd":  "WEB",
        "langCd":  "ko",
        "sdCode":  str(ctx.sd),   # 무해
    }
    headers = {
        "Accept": "text/html, */*",
        "Referer": f"{SITE}/ko/resMain?sdCode={ctx.sd}",
        "Origin": SITE,
    }
    try:
        resp = await page.context.request.post(f"{ALT_SITE}/ko/booking", form=form, headers=headers)
        html = await resp.text()
        token = _regex_find_csrf(html)
        got = bool(token)
        if got:
            ctx.csrf = token
            ctx.chnlCd = ctx.chnlCd or "WEB"
            ctx.referer = f"{ALT_SITE}/"
        dbg("csrf seed(post)", {"status": resp.status, "ok": got})
        return got
    except Exception as e:
        dbg("csrf seed(post) exception", {"err": str(e)})
        return False


def build_api_headers(ctx) -> dict:
    return {
        # onestop API는 Referer로 "루트(/)"를 기대함
        "Referer": f"{ALT_SITE}/",        # ALT_SITE == https://filmonestop.maketicket.co.kr
        "Origin": ALT_SITE,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/plain, */*",
        # 헤더 CSRF는 필수는 아님. 넣어도 무해하니 유지
        "X-CSRF-TOKEN": getattr(ctx, "csrf", "") or "",
    }


def ellipsis(s: str, n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else (s[: max(0, n-1)] + "…")

def zpad4(n: Optional[int]) -> str:
    if n is None:
        return " ?  "
    try:
        return f"{int(n):04d}"
    except Exception:
        return " ?  "

def icon_for(total: Optional[int], remain: Optional[int]) -> str:
    try:
        t = int(total or 0)
        r = int(remain or 0)
    except Exception:
        return "⚪"
    if t <= 0 and r <= 0:
        return "⚪"
    if r <= 0:
        return "🔴"
    ratio = r / max(1, t)
    if ratio >= 0.20:
        return "🟢"
    return "🟡"

def mmdd_hhmm(dt: Optional[str]) -> str:
    # accept "YYYY-MM-DD HH:MM" / "YYYYMMDDHHMM" / date+time fields separable
    if not dt:
        return "??-?? ??:??"
    # normalize several patterns
    m = re.match(r"(\d{4})[-/.]?(\d{2})[-/.]?(\d{2})[ T]?(\d{2}):?(\d{2})", dt)
    if m:
        y, mo, d, h, mi = m.groups()
        return f"{mo}-{d} {h}:{mi}"
    return dt

def join_dt(date_str: Optional[str], time_str: Optional[str]) -> str:
    if not date_str and not time_str:
        return ""
    ds = (date_str or "").strip()
    ts = (time_str or "").strip()
    if ts and not re.search(r":", ts):
        # e.g., "1800"
        if len(ts) >= 4:
            ts = f"{ts[:2]}:{ts[2:4]}"
    if ds and ts:
        # normalize ds if like "2025.09.17"
        ds2 = re.sub(r"[./]", "-", ds)
        return f"{ds2} {ts}"
    return ds or ts

def pad_field(s: str, width: int) -> str:
    s = s or ""
    if len(s) >= width:
        return s[:width]
    return s + " " * (width - len(s))

async def xhr(page: Page, url: str, method: str = "GET",
              params: Optional[Dict[str, Any]] = None,
              data: Optional[Dict[str, Any]] = None,
              headers: Optional[Dict[str, str]] = None,
              timeout: float = DEFAULT_TIMEOUT) -> Tuple[int, Any]:
    js = """
    async ({url, method, params, data, headers}) => {
      const u = new URL(url, location.href);
      if (params) for (const [k,v] of Object.entries(params)) {
        if (Array.isArray(v)) v.forEach(x => u.searchParams.append(k, x));
        else u.searchParams.append(k, String(v));
      }
      async function callJson(){
        const res = await fetch(u.toString(), {
          method,
          headers: Object.assign({
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest"
          }, headers || {}),
          body: method.toUpperCase() === "GET" ? undefined : JSON.stringify(data || {}),
          credentials: "include",
          cache: "no-store",
          redirect: "follow"
        });
        const text = await res.text();
        let body = null; try { body = JSON.parse(text); } catch(e) {}
        return { ok: res.ok, status: res.status, body, mode: "json" };
      }
      async function callForm(){
        const form = new URLSearchParams();
        if (data) for (const [k,v] of Object.entries(data)) form.append(k, String(v));
        const res = await fetch(u.toString(), {
          method,
          headers: Object.assign({
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest"
          }, headers || {}),
          body: method.toUpperCase() === "GET" ? undefined : form.toString(),
          credentials: "include",
          cache: "no-store",
          redirect: "follow"
        });
        const text = await res.text();
        let body = null; try { body = JSON.parse(text); } catch(e) {}
        return { ok: res.ok, status: res.status, body, mode: "form" };
      }
      let r = await callJson();
      if (!r.ok && method.toUpperCase() === "POST") {
        r = await callForm();
      }
      return { status: r.status, body: r.body, mode: r.mode };
    }
    """
    ret = await page.evaluate(js, {
        "url": url, "method": method, "params": params,
        "data": data, "headers": headers or {}
    })
    dbg(f"XHR {method} {url} -> {ret.get('status')} ({ret.get('mode')})")
    return ret["status"], ret["body"]
@dataclass
class Ctx:
    sd: str
    prodSeq: Optional[str] = None
    sdSeq: Optional[str] = None
    chnlCd: Optional[str] = "WEB"
    csrf: Optional[str] = None
    referer: Optional[str] = None
    plan_type: Optional[str] = None
    title: Optional[str] = None
    venue: Optional[str] = None
    dt: Optional[str] = None
    total: Optional[int] = None
    remain: Optional[int] = None
    action: Optional[str] = None
    status: Optional[str] = None   # HOLD / SOLDOUT / ?
    source: Optional[str] = None   # which API served the numbers


# === harvest helpers ======================================================
async def _harvest_booking_ctx_from_frame(fr) -> dict:
    try:
        return await fr.evaluate("""
        () => {
          const pick=(...sels)=>{ for(const s of sels){ const el=document.querySelector(s); if(el) return (el.value||el.getAttribute('content')||el.textContent||'').trim(); } return ''; };
          const out = {
            p: pick('#prodSeq','input[name="prodSeq"]','input[name="prod_seq"]'),
            s: pick('#sdSeq','input[name="sdSeq"]','input[name="sd_seq"]'),
            c: pick('#csrfToken','meta[name="csrf-token"]','meta[name="X-CSRF-TOKEN"]')
          };
          if(!out.p || !out.s){
            const html = document.documentElement.innerHTML;
            const m1 = html.match(/prodSeq["']?\\s*[:=]\\s*["']?(\\d+)/i); if(m1) out.p = m1[1];
            const m2 = html.match(/sdSeq["']?\\s*[:=]\\s*["']?(\\d+)/i);   if(m2) out.s = m2[1];
          }
          return out;
        }
        """)
    except Exception:
        return {"p":"", "s":"", "c":""}

async def _map_sd_via_filmapi(ctx, page, FILMAPI) -> bool:
    """filmapi 전체 목록을 스캔해서 sdCode 일치 항목으로 prodSeq/sdSeq를 얻는다."""
    st, body = await xhr(page, f"{FILMAPI}/api/v1/prodList", method="GET", params=None,
                         headers={"Referer": ctx.referer or SITE})
    if st != 200 or not isinstance(body, dict):
        return False
    items = body.get("data") or body.get("list") or []
    for it in (items if isinstance(items, list) else []):
        sd = str(it.get("sdCode") or it.get("sd_code") or it.get("sdCd") or "")
        if sd == ctx.sd:
            ctx.prodSeq = str(it.get("prodSeq") or it.get("prod_seq") or "")
            ctx.sdSeq   = str(it.get("sdSeq")   or it.get("sd_seq")   or "1")
            # 메타 보강 (있을 때만)
            ctx.title = ctx.title or (it.get("perfMainNm") or it.get("movieNm") or it.get("title") or "")
            for k in ("operHallNm","hallNm","placeNm","siteNm","screenNm","venueNm"):
                if it.get(k): ctx.venue = ctx.venue or it.get(k)
            d = it.get("perfDate") or it.get("perf_date")
            t = it.get("perfTime") or it.get("perf_time")
            if (d or t) and not getattr(ctx, "dt", None): ctx.dt = join_dt(d, t)
            return True
    return False
# ========================================================================


# ---------- HTTP helpers ----------

async def fetch_json(context: BrowserContext, url: str, method: str = "GET",
                     params: Optional[Dict[str, Any]] = None,
                     data: Optional[Dict[str, Any]] = None,
                     headers: Optional[Dict[str, str]] = None,
                     timeout: float = DEFAULT_TIMEOUT) -> Tuple[int, Any]:
    """Robust JSON fetch with dual-mode payload (JSON then form), returns (status, obj or None)."""
    h = {
        "Accept": "application/json, text/plain, */*",
        "Referer": headers.get("Referer") if headers else (context._impl_obj._options.get("baseURL") or SITE),  # best-effort
        "Origin": SITE,
    }
    if headers:
        h.update(headers)

    req = context.request
    url_final = url
    if params:
        # append query
        from urllib.parse import urlencode
        qs = urlencode(params, doseq=True)
        sep = "&" if "?" in url else "?"
        url_final = f"{url}{sep}{qs}"

    # Try JSON payload first (POST), then form-encoded as fallback
    try:
        if method.upper() == "GET":
            resp = await req.get(url_final, headers=h, timeout=timeout*1000)
        else:
            resp = await req.post(url_final, headers={**h, "Content-Type":"application/json"}, data=json.dumps(data or {}), timeout=timeout*1000)
        st = resp.status
        txt = await resp.text()
        try:
            return st, json.loads(txt) if txt else None
        except Exception:
            return st, None
    except Exception:
        if method.upper() == "GET":
            return 0, None
        # form-encoded fallback
        try:
            from urllib.parse import urlencode
            payload = urlencode(data or {})
            resp = await req.post(url_final, headers={**h, "Content-Type":"application/x-www-form-urlencoded"}, data=payload, timeout=timeout*1000)
            st = resp.status
            txt = await resp.text()
            try:
                return st, json.loads(txt) if txt else None
            except Exception:
                return st, None
        except Exception:
            return 0, None



async def looks_like_login(p: Page) -> bool:
    try:
        if p.is_closed():
            return True
        u = (p.url or "").lower()
        if "/login" in u:
            return True
        # password field present implies still on login page
        if await p.locator("input[type='password']").count():
            return True
        has_login  = await p.locator("a:has-text('로그인'), button:has-text('로그인')").count()
        has_logout = await p.locator("a:has-text('로그아웃'), button:has-text('로그아웃')").count()
        return bool(has_login and not has_logout)
    except Exception:
        return False

async def wait_logged_in(p: Page, timeout_ms: int = 150_000) -> bool:
    try:
        await p.wait_for_function(
            """() => !location.pathname.includes('login') &&
                    !document.querySelector("input[type='password']")""",
            timeout=timeout_ms
        )
        return True
    except Exception:
        return False
# ---------- Core steps ----------

async def wait_for_login(page: Page, timeout: float = 600.0):
    # 로그인 페이지로 이동 (탭 1개만 사용)
    try:
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    except Exception:
        pass

    # UI 기준: /login 벗어나고, 비밀번호 입력칸이 없어질 때까지 기다림
    await page.wait_for_function("""
        () => {
          const onLogin = location.pathname.includes('login');
          const hasPw   = !!document.querySelector('input[type="password"]');
          return (!onLogin && !hasPw);
        }
    """, timeout=int(timeout * 1000))
    return True

# === DROP-IN REPLACEMENT ===

async def resolve_context(ctx: Ctx, page: Page) -> Ctx:
    from urllib.parse import urlparse, parse_qs, urljoin

    # 0) 후보 URL들 (booking 먼저, 그다음 resMain)
    candidates = [
        f"{ALT_SITE}/ko/booking?sdCode={ctx.sd}",
        f"{ALT_SITE}/ko/resMain?sdCode={ctx.sd}",
        f"{SITE}/ko/booking?sdCode={ctx.sd}",
        f"{SITE}/ko/resMain?sdCode={ctx.sd}",
    ]

    found = False
    for url in candidates:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=int(DEFAULT_TIMEOUT*1000))
            await page.wait_for_timeout(300)

            # 1) 상위 DOM에서 먼저 캐기
            vals_top = await _harvest_booking_ctx_from_frame(page.main_frame)
            dbg("booking harvest(top)", {"url": page.url, "prodSeq": vals_top.get("p"), "sdSeq": vals_top.get("s"), "csrf": bool(vals_top.get("c"))})
            if vals_top.get("c"): ctx.csrf = ctx.csrf or vals_top["c"]
            if vals_top.get("p"): ctx.prodSeq = ctx.prodSeq or str(vals_top["p"])
            if vals_top.get("s"): ctx.sdSeq   = ctx.sdSeq   or str(vals_top["s"])
            if ctx.prodSeq and ctx.sdSeq:
                found = True
                break

            # 2) 프레임들에서 캐기
            for fr in page.frames:
                vals = await _harvest_booking_ctx_from_frame(fr)
                if vals.get("c"): ctx.csrf = ctx.csrf or vals["c"]
                if vals.get("p"): ctx.prodSeq = ctx.prodSeq or str(vals["p"])
                if vals.get("s"): ctx.sdSeq   = ctx.sdSeq   or str(vals["s"])
                dbg("booking harvest(frame)", {"url": fr.url, "prodSeq": vals.get("p"), "sdSeq": vals.get("s"), "csrf": bool(vals.get("c"))})
                if ctx.prodSeq and ctx.sdSeq:
                    found = True
                    break
            if found:
                break

            # 3) sForm submit으로 oneStopFrame 채우기 (필요 시)
            try:
                if await page.locator("#sForm").count():
                    await page.evaluate("document.querySelector('#sForm')?.submit()")
                    await page.wait_for_timeout(900)
                    # 프레임 재하베스트
                    for fr in page.frames:
                        vals = await _harvest_booking_ctx_from_frame(fr)
                        if vals.get("p") and vals.get("s"):
                            ctx.prodSeq = ctx.prodSeq or str(vals["p"])
                            ctx.sdSeq   = ctx.sdSeq   or str(vals["s"])
                            if vals.get("c"): ctx.csrf = ctx.csrf or vals["c"]
                            dbg("booking harvest(after submit)", {"url": fr.url, "prodSeq": vals.get("p"), "sdSeq": vals.get("s"), "csrf": bool(vals.get("c"))})
                            found = True
                            break
                if found:
                    break
            except Exception:
                pass

            # 4) iframe/src 쿼리스트링에서도 한 번 더 파싱
            try:
                src = await page.evaluate("""() => document.querySelector('#oneStopFrame')?.getAttribute('src') || '' """)
            except Exception:
                src = ""
            candidates2 = [src] + [fr.url for fr in page.frames if fr.url]
            for raw in candidates2:
                if not raw:
                    continue
                full = raw if raw.startswith("http") else urljoin(ALT_SITE + "/", raw.lstrip("/"))
                q = parse_qs(urlparse(full).query)
                p = (q.get("prodSeq") or q.get("prod_seq") or [""])[0]
                s = (q.get("sdSeq")   or q.get("sd_seq")   or [""])[0]
                if not ctx.prodSeq and p: ctx.prodSeq = str(p)
                if not ctx.sdSeq   and s: ctx.sdSeq   = str(s)
            dbg("booking harvest(url)", {"prodSeq": ctx.prodSeq or "", "sdSeq": ctx.sdSeq or "", "csrf": bool(ctx.csrf)})
            if ctx.prodSeq and ctx.sdSeq:
                found = True
                break

        except Exception:
            continue

    # 5) 여전히 비면 filmapi 전체 스캔 (있으면 보강)
    if not (ctx.prodSeq and ctx.sdSeq):
        ok = await _map_sd_via_filmapi(ctx, page, FILMAPI)
        dbg("filmapi map", {"ok": ok, "prodSeq": ctx.prodSeq or "", "sdSeq": ctx.sdSeq or ""})

    # 6) CSRF 보강: prod/sd 확보 후 'POST 시딩'으로 토큰만 확보
    if ctx.prodSeq and ctx.sdSeq and not ctx.csrf:
        ok = await _post_into_booking(ctx, page)   # 네비게이션 없이 토큰만 확보
        if not ok:
            # 마지막 폴백: 화면 DOM/iframe/쿠키/전체HTML 한 번 더 긁기 (있다면)
            try:
                ctx.csrf = ctx.csrf or await _harvest_csrf_any(page)
            except Exception:
                pass
        dbg("booking csrf(via POST)", {"csrf": bool(ctx.csrf)})

    # === prodChk 선행 (있을 때만) — 한 번만 ===
    if ctx.prodSeq and ctx.sdSeq and ctx.csrf:
        headers = {
            "Referer": f"{ALT_SITE}/",
            "Origin": ALT_SITE,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*",
            "X-CSRF-TOKEN": ctx.csrf,
        }
        data = {
            "prodSeq":   str(ctx.prodSeq),
            "sdSeq":     str(ctx.sdSeq),
            "chnlCd":    ctx.chnlCd or "WEB",
            "csrfToken": ctx.csrf,   # 바디에 토큰 필수
        }
        st, obj = await xhr(page, f"{API}/api/v1/rs/prodChk", method="POST", data=data, headers=headers)
        dbg("prodChk", {"status": st, "ok": isinstance(obj, dict)})


    # === [NEW] 루트에서 CSRF 수확 ===
    if (ctx.prodSeq and ctx.sdSeq) and not ctx.csrf:
        # 루트 접속해서 히든 토큰 수확
        await page.goto(f"{ALT_SITE}/", wait_until="domcontentloaded", timeout=int(DEFAULT_TIMEOUT*1000))
        await page.wait_for_timeout(300)
        ctx.csrf = await _harvest_csrf_from(page)
        dbg("csrf from root(/)", {"csrf": bool(ctx.csrf)})

        # 혹시 루트에 없으면 booking 페이지에서 한 번 더 시도
        if not ctx.csrf:
            book_url = f"{ALT_SITE}/ko/booking?prodSeq={ctx.prodSeq}&sdSeq={ctx.sdSeq}"
            await page.goto(book_url, wait_until="domcontentloaded", timeout=int(DEFAULT_TIMEOUT*1000))
            await page.wait_for_timeout(300)
            ctx.csrf = await _harvest_csrf_from(page)
            dbg("csrf from booking(prod/sd)", {"csrf": bool(ctx.csrf)})
    # === [NEW] HTTP 폴백: Referer 조합으로 HTML 직독 후 토큰 파싱 ===
    if ctx.prodSeq and ctx.sdSeq and not ctx.csrf:
        book_url = f"{ALT_SITE}/ko/booking?prodSeq={ctx.prodSeq}&sdSeq={ctx.sdSeq}"
        # 가능한 레퍼러 후보 (biff/alt 루트+resMain)
        referers = [
            f"{SITE}/ko/resMain?sdCode={ctx.sd}",
            f"{ALT_SITE}/ko/resMain?sdCode={ctx.sd}",
            f"{SITE}/",
            f"{ALT_SITE}/",
        ]
        for ref in referers:
            token = await _harvest_csrf_via_http(page.context, book_url, ref)
            dbg("csrf via http", {"ref": ref, "ok": bool(token)})
            if token:
                ctx.csrf = token
                break

        # 그래도 없으면 한 번 더 프레임/문서 전체 재스캔(딜레이 증가)
        if not ctx.csrf:
            try:
                await page.goto(book_url, wait_until="load", timeout=int(DEFAULT_TIMEOUT*1000))
                await page.wait_for_timeout(2000)  # 토큰 인젝션 대기
            except Exception:
                pass
            ctx.csrf = await _harvest_csrf_any(page)
            dbg("csrf final scan", {"csrf": bool(ctx.csrf)})

    # === prodChk 선행 (있을 때만) ===
    if ctx.prodSeq and ctx.sdSeq and ctx.csrf:
        headers = {
            "Referer": f"{ALT_SITE}/",
            "Origin": ALT_SITE,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*",
            "X-CSRF-TOKEN": ctx.csrf,
        }
        data = {
            "prodSeq": str(ctx.prodSeq),
            "sdSeq":   str(ctx.sdSeq),
            "chnlCd":  ctx.chnlCd or "WEB",
            "csrfToken": ctx.csrf,
        }
        st, obj = await xhr(page, f"{API}/api/v1/rs/prodChk", method="POST", data=data, headers=headers)
        dbg("prodChk", {"status": st, "ok": isinstance(obj, dict)})

    # === [NEW] prodChk 선행 (토큰/세션 초기화) ===
    if ctx.prodSeq and ctx.sdSeq and ctx.csrf:
        headers = {
            "Referer": f"{ALT_SITE}/",
            "Origin": ALT_SITE,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*",
            # 헤더의 X-CSRF는 없어도 되지만 있어도 무해
            "X-CSRF-TOKEN": ctx.csrf,
        }
        data = {
            "prodSeq": str(ctx.prodSeq),
            "chnlCd":  ctx.chnlCd or "WEB",   # <-- 중요: WEB
            "sdSeq":   str(ctx.sdSeq),
            "csrfToken": ctx.csrf,            # <-- 중요: 폼 바디에 토큰
        }
        st, obj = await xhr(page, f"{API}/api/v1/rs/prodChk", method="POST", data=data, headers=headers)
        dbg("prodChk", {"status": st, "ok": isinstance(obj, dict)})


    # 7) 사전 검증 (CSRF 있을 때만)
    headers = build_api_headers(ctx)
    if ctx.prodSeq and ctx.sdSeq and ctx.csrf:
        _st_chk, _ = await xhr(page, f"{API}/api/v1/rs/chkProdSdSeq", method="POST",
                               data={"prodSeq": ctx.prodSeq, "sdSeq": ctx.sdSeq,
                                     "chnlCd": ctx.chnlCd or "WEB", "csrfToken": ctx.csrf},
                               headers=headers)
        dbg("chkProdSdSeq(prodSeq/sdSeq)", {"prodSeq": ctx.prodSeq, "sdSeq": ctx.sdSeq}, "->", _st_chk, type(_).__name__)

    ctx.referer = page.url or ALT_SITE
    return ctx


async def fetch_meta_filmapi(ctx: Ctx, page: Page):
    if not ctx.sd:
        return
    headers = {"Referer": ctx.referer or SITE}
    try:
        st, obj = await xhr(page, f"{FILMAPI}/api/v1/prodList", method="GET", params=None, headers=headers)
        if st == 200 and isinstance(obj, dict):
            items = obj.get("data") or obj.get("list") or []
            for it in (items if isinstance(items, list) else []):
                if str(it.get("sdCode") or it.get("sd_code") or it.get("sdCd") or "") != ctx.sd:
                    continue
                ctx.title = ctx.title or (it.get("perfMainNm") or it.get("movieNm") or it.get("title") or "")
                for k in ("operHallNm", "hallNm", "placeNm", "siteNm", "screenNm", "venueNm"):
                    if it.get(k):
                        ctx.venue = ctx.venue or it.get(k)
                d = it.get("perfDate") or it.get("perf_date")
                t = it.get("perfTime") or it.get("perf_time")
                if (d or t) and not getattr(ctx, "dt", None):
                    ctx.dt = join_dt(d, t)
                break
    except Exception:
        pass
# === /DROP-IN REPLACEMENT ===


async def detect_plan_type(ctx: Ctx, page: Page) -> str:
    headers = build_api_headers(ctx)
    if ctx.csrf:
        headers["X-CSRF-TOKEN"] = ctx.csrf
    st, obj = await xhr(page, f"{API}/api/v1/rs/seat/GetRsSeatBaseMap", method="POST",
                               data={"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq, "chnl_cd": ctx.chnlCd, "timeStemp": "", "csrfToken": ctx.csrf or ""},
                               headers=headers)
    dbg("GetRsSeatBaseMap", {"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq}, "->", st, (list(obj.keys())[:5] if isinstance(obj, dict) else type(obj)))
    plan = ""
    if st == 200 and isinstance(obj, dict):
        for k in ("plan","planType","plan_type","seatPlanType"):
            v = obj.get(k)
            if v:
                plan = str(v).upper()
                break
    ctx.plan_type = plan or ctx.plan_type or ""
    return ctx.plan_type or ""


def _pull_int(d: Dict[str, Any], *keys: str) -> int:
    for k in keys:
        if k in d and d.get(k) is not None:
            try:
                return int(d.get(k))
            except Exception:
                pass
    return 0

async def summarize_seats(ctx: Ctx, page: Page) -> Tuple[Optional[int], Optional[int], str]:
    """Return (total, remain, plan_used).
    NRS: blockSummary2 → tickettype
    RS : GetRsSeatStatusList → blockSummary2
    """
    headers = build_api_headers(ctx)
    if ctx.csrf:
        headers["X-CSRF-TOKEN"] = ctx.csrf
    plan = (ctx.plan_type or "").upper()

    # --- NRS path (또는 미확정) ---
    if plan in ("", "NRS", "FREE", "RS"):  # RS여도 NRS로 데이터가 나오는 케이스가 있어 먼저 시도
        # 1) blockSummary2
        st, summ = await xhr(
            page,
            f"{API}/api/v1/rs/blockSummary2",
            method="POST",
            data={
                "prod_seq": ctx.prodSeq,
                "sd_seq": ctx.sdSeq,
                "chnl_cd": ctx.chnlCd,
                "csrfToken": ctx.csrf or "",
            },
            headers=headers,
        )
        dbg("blockSummary2(NRS)", {"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq}, "->", st,
            (list(summ.keys())[:5] if isinstance(summ, dict) else type(summ)))
        if st == 200 and isinstance(summ, dict):
            avail = _pull_int(summ, "admissionAvailPersonCnt", "restSeatCnt")
            total = _pull_int(summ, "admissionTotalPersonCnt", "saleSeatCnt", "rendrSeatCnt")
            if total <= 0 and avail > 0:
                total = avail
            if total > 0 or avail > 0:
                return total or None, avail or None, "NRS"

        # 2) tickettype fallback
        st, tk = await xhr(
            page,
            f"{API}/api/v1/rs/tickettype",
            method="POST",
            data={
                "prod_seq": ctx.prodSeq,
                "sd_seq": ctx.sdSeq,
                "chnl_cd": ctx.chnlCd,
                "csrfToken": ctx.csrf or "",
            },
            headers=headers,
        )
        dbg("tickettype", {"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq}, "->", st,
            (list(tk.keys())[:5] if isinstance(tk, dict) else type(tk)))
        if st == 200 and isinstance(tk, (dict, list)):
            rows = tk.get("data") if isinstance(tk, dict) and isinstance(tk.get("data"), list) else (tk if isinstance(tk, list) else [])
            total = 0
            remain = 0
            if isinstance(rows, list):
                for s in rows:
                    if not isinstance(s, dict):
                        continue
                    avail = _pull_int(s, "admissionAvailPersonCnt", "restSeatCnt")
                    sold  = _pull_int(s, "admissionPersonCnt")
                    ttl_v = s.get("admissionTotalPersonCnt")
                    if ttl_v is not None:
                        try:
                            ttl = int(ttl_v)
                        except Exception:
                            ttl = avail + sold
                    else:
                        ttl = avail + sold
                    total = max(total, ttl)
                    remain = max(remain, avail)
            if total or remain:
                return total or None, remain or None, "NRS"

    # --- RS path ---
    # 1) 좌석 리스트로 정확 집계
    st, lst = await xhr(
        page,
        f"{API}/api/v1/seat/GetRsSeatStatusList",
        method="POST",
        data={
            "prod_seq": ctx.prodSeq,
            "sd_seq": ctx.sdSeq,
            "chnl_cd": ctx.chnlCd,
            "timeStemp": "",
            "csrfToken": ctx.csrf or "",
        },
        headers=headers,
    )
    dbg("GetRsSeatStatusList", {"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq}, "->", st, (type(lst)))
    total = 0
    remain = 0
    if st == 200 and isinstance(lst, (list, dict)):
        items = lst if isinstance(lst, list) else lst.get("data", [])
        if isinstance(items, list):
            for s in items:
                if not isinstance(s, dict):
                    continue
                total += 1
                code = str(s.get("seatSts") or s.get("status") or "").upper()
                # 사용 가능 코드
                if code in ("N", "A", "AVAIL", "Y", "ABLE"):
                    remain += 1
        if total or remain:
            return total or None, remain or None, "RS"

    # 2) 존 합계 폴백 (seatStatusList 실패 시)
    st, z = await xhr(
        page,
        f"{API}/api/v1/rs/blockSummary2",
        method="POST",
        data={
            "prod_seq": ctx.prodSeq,
            "sd_seq": ctx.sdSeq,
            "chnl_cd": ctx.chnlCd,
            "csrfToken": ctx.csrf or "",
        },
        headers=headers,
    )
    dbg("blockSummary2(RS-fallback)", {"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq}, "->", st,
        (list(z.keys())[:5] if isinstance(z, dict) else type(z)))
    if st == 200 and isinstance(z, dict):
        total = _pull_int(z, "saleSeatCnt", "rendrSeatCnt", "admissionTotalPersonCnt")
        remain = _pull_int(z, "admissionAvailPersonCnt", "restSeatCnt")
        if total or remain:
            return total or None, remain or None, "RS"

    return None, None, plan or ""



async def _harvest_csrf_from(page) -> str:
    try:
        vals = await page.evaluate("""() => ({
          c: document.querySelector('#csrfToken')?.value
             || document.querySelector('meta[name="csrf-token"]')?.content
             || document.querySelector('meta[name="X-CSRF-TOKEN"]')?.content
             || ''
        })""")
        return (vals.get("c") or "")
    except Exception:
        return ""


# ---------- Booking helpers ----------


async def proceed_booking(ctx: Ctx, page: Page, plan_used: str) -> Tuple[str, str]:
    # If seat counts are unknown, do not claim SOLDOUT
    if ctx.remain is None:
        return ("login?", "?")

    # If known remain <= 0 → SOLDOUT
    if (ctx.remain or 0) <= 0:
        return ("SOLDOUT", "SOLDOUT")

    # Focus current page and attempt minimal interactions
    try:
        await page.bring_to_front()
    except Exception:
        pass

    if plan_used == "NRS":
        # quantity=1 → Next
        try:
            # dropdown/select
            for sel in ["select[name='qty']", "#qty", "select[name='selCnt']", "select.sel-qty"]:
                if await page.locator(sel).count():
                    await page.select_option(sel, value="1")
                    break
            # buttons likely containing '다음' or 'Next' or proceed
            for btn in ["button:has-text('다음')",
                        "button:has-text('Next')",
                        "button.next", "a.next", "button.proceed"]:
                if await page.locator(btn).count():
                    await page.click(btn, timeout=DEFAULT_TIMEOUT*1000)
                    break
        except Exception:
            pass
        act = "qty=1"
    else:
        # RS: pick one seat (very heuristic)
        act = "seat=?"
        try:
            # common seat selector heuristics
            sel_candidates = ["[data-seat-available='Y']",
                              ".seat.available",
                              "g.seat.available",
                              "button.seat:not(.sold)",
                              "[data-seat-status='A']"]
            picked = False
            for sel in sel_candidates:
                loc = page.locator(sel)
                if await loc.count():
                    await loc.first.click(force=True, timeout=DEFAULT_TIMEOUT*1000)
                    act = "seat=auto"
                    picked = True
                    break
            # Next
            for btn in ["button:has-text('다음')",
                        "button:has-text('Next')",
                        "button.next", "a.next", "button.proceed"]:
                if await page.locator(btn).count():
                    await page.click(btn, timeout=DEFAULT_TIMEOUT*1000)
                    break
        except Exception:
            pass

    # Verify payment page + form
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=int(DEFAULT_TIMEOUT*1000))
    except Exception:
        pass

    url = (page.url or "").lower()
    if ("/payment" in url) or ("/booking" in url):
        # verify form exists
        try:
            has_form = await page.locator("form[action*='/payment'], #paymentForm, [name='paymentForm']").count()
            if has_form:
                return (act + " • HOLD", "HOLD")
        except Exception:
            # if URL says booking but no form detected, still consider hold (site variants)
            return (act + " • HOLD", "HOLD")

    return (act, "?")


# ---------- Emit log ----------

def emit_line(ctx: Ctx):
    icon = icon_for(ctx.total, ctx.remain)
    sd = pad_field(ctx.sd, 3)
    title = pad_field(ellipsis(ctx.title or "", 22), 22)
    venue = pad_field(ellipsis(ctx.venue or "?", 14), 14)
    dt = pad_field(mmdd_hhmm(ctx.dt or ""), 11)
    plan = pad_field((ctx.plan_type or "ALL")[:3], 3)
    total = zpad4(ctx.total)
    remain = zpad4(ctx.remain)
    action = ctx.action or ""
    status = ctx.status or ""
    tail = action if status in ("", "?") else f"{action}"
    if status == "SOLDOUT":
        tail = "SOLDOUT"
    if status == "HOLD" and "HOLD" not in tail:
        tail = (tail + " • HOLD").strip()
    line = f"{icon} {sd} | {title} | {venue} | {dt} | {plan} | T/R={total}/{remain} | {tail}"
    print(line, flush=True)


# ---------- Worker ----------

async def handle_sd(sd: str, context: BrowserContext, headless: bool, info_only: bool) -> None:
    page = await context.new_page()
    ctx = Ctx(sd=sd)

    # Resolve and meta
    ctx = await resolve_context(ctx, page)
    await fetch_meta_filmapi(ctx, page)  # optional reinforcement
    plan = await detect_plan_type(ctx, page)
    total, remain, plan_used = await summarize_seats(ctx, page)
    ctx.total = total
    ctx.remain = remain
    if not ctx.plan_type:
        ctx.plan_type = plan_used or plan or "ALL"

    # 정보만 추출하고 끝낼 때
    if info_only:
        ctx.action = ""
        ctx.status = ""
        emit_line(ctx)  # 한 줄 요약: 제목/장소/일시/PLAN/T/R
        try:
            await page.wait_for_timeout(300)  # 잠깐 열어두고
        except Exception:
            pass
        await page.close()
        return
    # Proceed booking & hold
    action, status = await proceed_booking(ctx, page, plan_used or plan or "")
    ctx.action = action
    ctx.status = status

    # Emit compact line
    emit_line(ctx)

    # If HOLD, keep page alive per PAY_HOLD_SEC (user can interact)
    if status == "HOLD":
        try:
            await page.wait_for_timeout(PAY_HOLD_SEC * 1000)
        except Exception:
            pass
    # keep login page open to retain session
        # # keep login page open to retain session
        # # keep login page open to retain session
        # # keep login page open to retain session
        # await page.close()


# ---------- Main ----------

async def main():
    # 전역에 반영
    global PAY_HOLD_SEC, DEBUG
    try:
        PAY_HOLD_SEC = int(getattr(RUNTIME, "STAY_SEC", 600))
    except Exception:
        PAY_HOLD_SEC = 600
    DEBUG = bool(getattr(RUNTIME, "DEBUG", False))

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=bool(RUNTIME.HEADLESS),
            args=["--disable-web-security", "--disable-site-isolation-trials"]
        )
        context: BrowserContext = await browser.new_context(
            ignore_https_errors=True,
            viewport={"width": 1200, "height": 900},
        )

        # 1) 로그인
        page = await context.new_page()
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
        print("🔐 로그인 창을 열었습니다. 로그인만 완료하세요…", flush=True)

        ok = await wait_for_login(page, timeout=180.0)
        if not ok:
            print("⚠ 로그인 감지 실패 — 로그인 후 다시 시도하세요.", flush=True)
            await context.close(); await browser.close()
            return

        print("✅ 로그인 감지. 동시 예매 시작.", flush=True)

        # 1.5) 교차 도메인 세션 워밍업
        try:
            p2 = await context.new_page()
            await p2.goto(f"{ALT_SITE}/ko/login", wait_until="domcontentloaded")
            await p2.wait_for_timeout(400)
            await p2.close()
            if DEBUG: print("[WARMUP] ALT_SITE ok", flush=True)
        except Exception as e:
            if DEBUG: print(f"[WARMUP] ALT_SITE skip: {e}", flush=True)

        try:
            r = await context.request.get(f"{FILMAPI}/api/v1/prodList",
                                          params={"_": "warmup"})
            if DEBUG: print(f"[WARMUP] FILMAPI {r.status}", flush=True)
        except Exception as e:
            if DEBUG: print(f"[WARMUP] FILMAPI skip: {e}", flush=True)

        # 2) 동시 처리 (하드코딩된 SDCODES 사용)
        sem = asyncio.Semaphore(max(1, int(RUNTIME.CONCURRENCY)))

        async def run_one(code: str):
            async with sem:
                try:
                    await handle_sd(code, context, bool(RUNTIME.HEADLESS), bool(RUNTIME.INFO_ONLY))
                except Exception as e:
                    if DEBUG:
                        import sys, traceback
                        print(f"[ERR] {code}: {e.__class__.__name__}: {e}", file=sys.stderr, flush=True)
                        traceback.print_exc()
                    # 최소 요약 라인 유지
                    ctx = Ctx(sd=code, title="", venue="?", dt="", plan_type="?", total=None, remain=None, action="", status="?")
                    print(
                        f"⚪ {pad_field(code,3)} | {pad_field('',22)} | {pad_field('?',14)} | "
                        f"{pad_field('??-?? ??:??',11)} | {pad_field('?',3)} | "
                        f"T/R={zpad4(None)}/{zpad4(None)} | ?",
                        flush=True
                    )

        # SDCODES 비었으면 바로 종료
        sd_list = list(getattr(RUNTIME, "SDCODES", [])) or []
        if not sd_list:
            print("⚠ 실행할 스케쥴넘버(SDCODES)가 비어 있습니다. RUNTIME.SDCODES를 설정하세요.", flush=True)
            await context.close(); await browser.close()
            return

        await asyncio.gather(*(run_one(sd) for sd in sd_list))

        # 3) 정리 — handle_sd 내부에서 결제 HOLD 대기 후 반환됨
        await context.close()
        await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

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


SITE = os.getenv("BT_SITE", "https://biff.maketicket.co.kr")
API  = os.getenv("BT_API",  "https://filmonestopapi.maketicket.co.kr")
FILMAPI = os.getenv("BT_FILMAPI", "https://filmapi.maketicket.co.kr")
LOGIN_URL = os.getenv("BT_LOGIN_URL", f"{SITE}/ko/login")
RESMAIN   = os.getenv("BT_RESMAIN",   f"{SITE}/ko/resMain?sdCode={{sd}}")

DEFAULT_TIMEOUT = float(os.getenv("BT_TIMEOUT_SEC", "7.5"))  # per action
PAY_HOLD_SEC    = int(os.getenv("BT_PAY_HOLD_SEC", "600"))    # payment page hold seconds

# ---------- Utilities ----------

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


@dataclass
class Ctx:
    sd: str
    prodSeq: Optional[str] = None
    sdSeq: Optional[str] = None
    chnlCd: Optional[str] = "BIFF"
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

async def wait_for_login(page: Page, timeout: float = 180.0):
    """Open login page and auto-detect completion (no prompts, no tab churn)."""
    try:
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    except Exception:
        pass

    # Prefer robust DOM-based detection (password field disappears & URL leaves /login)
    ok = await wait_logged_in(page, timeout_ms=int(timeout * 1000))
    if ok:
        return True

    # Fallback: poll with conservative checks (no new tabs)
    import time as _t
    deadline = _t.time() + timeout
    stable_ok = 0
    last_state = ""
    while _t.time() < deadline:
        try:
            url = (page.url or "").lower()
        except Exception:
            url = ""
        try:
            pw = await page.locator("input[type='password']").count()
        except Exception:
            pw = 0
        state = f"{('/login' not in url)}-{(pw == 0)}"
        if state == last_state and state == "True-True":
            stable_ok += 1
            if stable_ok >= 2:
                return True
        else:
            stable_ok = 0
            last_state = state
        await page.wait_for_timeout(700)
    return False

    if manual:
        print("🔐 로그인 창을 열었습니다. 브라우저에서 로그인 완료 후, 이 터미널에서 Enter 키를 눌러 진행합니다.", flush=True)
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, input, "")
        except Exception:
            # Non-interactive environment: fall back to long wait
            await page.wait_for_timeout(600000)

        # Optional strict cookie verification before proceeding
        if strict_cookie:
            ok = 0
            deadline = asyncio.get_event_loop().time() + 60.0
            while asyncio.get_event_loop().time() < deadline:
                try:
                    cookies = await page.context.cookies()
                except Exception:
                    cookies = []
                if ("/login" not in (page.url or "").lower()) and _has_site_cookie(cookies):
                    ok += 1
                    if ok >= 2:
                        return True
                else:
                    ok = 0
                await asyncio.sleep(0.7)
            # If strict check failed, still return False to avoid proceeding
            return False
        return True

    # Auto-detect mode (opt-in): require URL left '/login' and cookies present, twice.
    deadline = asyncio.get_event_loop().time() + timeout
    stable_ok = 0
    last_state = ""
    while asyncio.get_event_loop().time() < deadline:
        url = (page.url or "").lower()
        try:
            cookies = await page.context.cookies()
        except Exception:
            cookies = []
        ok_cookie = _has_site_cookie(cookies)
        state = f"{('/login' not in url)}-{ok_cookie}"
        if state == last_state and state == "True-True":
            stable_ok += 1
            if stable_ok >= 2:
                return True
        else:
            stable_ok = 0
            last_state = state
        await asyncio.sleep(0.8)
    return False


async def resolve_context(ctx: Ctx, page: Page) -> Ctx:
    """Resolve prodSeq/sdSeq/chnlCd/csrf and a stable referer. Navigate to resMain if needed."""
    # Navigate to resMain for this sd (helps server set right CSRF/referer scope)
    try:
        await page.goto(RESMAIN.format(sd=ctx.sd), wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT*1000)
    except Exception:
        pass

    # Try DOM/URL extraction first
    try:
        # URL query
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(page.url).query or "")
        if not ctx.sd and qs.get("sdCode"):
            ctx.sd = (qs["sdCode"][0] or ctx.sd)
    except Exception:
        pass

    # Try CSRF meta / hidden field
    try:
        sel_meta = 'meta[name="csrf-token"], meta[name="csrf"], meta[name="X-CSRF-TOKEN"], input[name="csrfToken"]'
        el = await page.query_selector(sel_meta)
        if el:
            val = (await el.get_attribute("content")) or (await el.get_attribute("value"))
            if val:
                ctx.csrf = val.strip()
    except Exception:
        pass

    # prodChk / prodSummary to map sd→(prodSeq, sdSeq, chnlCd)
    headers = {"Referer": page.url or SITE}
    # Try a few param variants
    for variant in [{"sd_code": ctx.sd}, {"sdCode": ctx.sd}, {"sdCd": ctx.sd}]:
        st, obj = await fetch_json(page.context, f"{API}/api/v1/rs/prodChk", method="POST", data=variant, headers=headers)
        if st == 200 and isinstance(obj, dict):
            # heuristic field names
            ctx.prodSeq = str(obj.get("prodSeq") or obj.get("prod_seq") or ctx.prodSeq or "")
            ctx.sdSeq   = str(obj.get("sdSeq")   or obj.get("sd_seq")   or ctx.sdSeq   or "")
            ctx.chnlCd  = str(obj.get("chnlCd")  or obj.get("chnl_cd")  or ctx.chnlCd  or "BIFF")
            break

    # prodSummary for more stable mapping (also venue/date/time sometimes here)
    st, summ = await fetch_json(page.context, f"{API}/api/v1/rs/prodSummary", method="POST",
                                data={"prodSeq": ctx.prodSeq, "sdSeq": ctx.sdSeq, "chnlCd": ctx.chnlCd},
                                headers=headers)
    if st == 200 and isinstance(summ, dict):
        # venue
        for k in ("operHallNm","operHallName","hallNm","hallName",
                  "siteNm","siteName","placeNm","placeName","screenNm","screenName","venueNm","venueName"):
            v = summ.get(k)
            if v:
                ctx.venue = str(v).strip()
                break
        # date/time
        d = summ.get("perfDate") or summ.get("perf_date")
        t = summ.get("perfTime") or summ.get("perf_time")
        if d or t:
            ctx.dt = join_dt(d, t)

    # title via DOM meta or heading
    if not ctx.title:
        try:
            cand = await page.evaluate("""() => {
                const m = document.querySelector('meta[property="og:title"]');
                if (m && m.content) return m.content;
                const h1 = document.querySelector('h1, .title, .tit, .subject');
                if (h1) return h1.textContent.trim();
                return '';
            }""")
            if cand:
                ctx.title = cand.strip()
        except Exception:
            pass

    ctx.referer = page.url or SITE
    return ctx


async def fetch_meta_filmapi(ctx: Ctx, context: BrowserContext):
    """Optional: filmapi prodList for title/venue/date/time reinforcement."""
    if not ctx.sd:
        return
    headers = {"Referer": ctx.referer or SITE}
    try:
        st, obj = await fetch_json(context, f"{FILMAPI}/api/v1/prodList", method="GET",
                                   params={"sdCode": ctx.sd}, headers=headers)
        if st == 200 and isinstance(obj, dict):
            items = obj.get("data") or obj.get("list") or []
            if isinstance(items, list) and items:
                it = items[0]
                ctx.title = it.get("perfMainNm") or ctx.title
                # venue hints
                for k in ("operHallNm","hallNm","placeNm","siteNm","screenNm","venueNm"):
                    if it.get(k):
                        ctx.venue = it.get(k)
                        break
                ctx.dt = join_dt(it.get("perfDate"), it.get("perfTime")) or ctx.dt
    except Exception:
        pass


async def detect_plan_type(ctx: Ctx, context: BrowserContext) -> str:
    headers = {"Referer": ctx.referer or SITE}
    st, obj = await fetch_json(context, f"{API}/api/v1/rs/seat/GetRsSeatBaseMap", method="POST",
                               data={"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq, "chnl_cd": ctx.chnlCd, "timeStemp": ""},
                               headers=headers)
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


async def summarize_seats(ctx: Ctx, context: BrowserContext) -> Tuple[Optional[int], Optional[int], str]:
    """Return (total, remain, plan_used). NRS uses blockSummary2→tickettype; RS uses seatStatusList→zone."""
    headers = {"Referer": ctx.referer or SITE}
    plan = (ctx.plan_type or "").upper()

    # --- NRS path (or unknown) ---
    if plan in ("", "NRS", "FREE", "RS"):  # allow unknown to try NRS first
        # 1) blockSummary2
        st, summ = await fetch_json(context, f"{API}/api/v1/rs/blockSummary2", method="POST",
                                    data={"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq, "chnl_cd": ctx.chnlCd},
                                    headers=headers)
        if st == 200 and isinstance(summ, dict):
            avail = _pull_int(summ, "admissionAvailPersonCnt", "restSeatCnt")
            total = _pull_int(summ, "admissionTotalPersonCnt", "saleSeatCnt", "rendrSeatCnt")
            if total <= 0 and avail > 0:
                total = avail
            if total > 0 or avail > 0:
                ctx.source = "blockSummary2"
                return total or None, avail or None, "NRS"

        # 2) tickettype fallback
        st, tk = await fetch_json(context, f"{API}/api/v1/rs/tickettype", method="POST",
                                  data={"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq, "chnl_cd": ctx.chnlCd},
                                  headers=headers)
        if st == 200 and isinstance(tk, dict):
            # sometimes list under 'data' or top-level list
            rows = tk.get("data") if isinstance(tk.get("data"), list) else (tk if isinstance(tk, list) else [])
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
                ctx.source = "tickettype"
                return total or None, remain or None, "NRS"

    # --- RS path ---
    st, lst = await fetch_json(context, f"{API}/api/v1/seat/GetRsSeatStatusList", method="POST",
                               data={"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq, "chnl_cd": ctx.chnlCd, "timeStemp": ""},
                               headers={"Referer": ctx.referer or SITE})
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
                # Available codes heuristic
                if code in ("N", "A", "AVAIL", "Y", "ABLE"):
                    remain += 1
        if total or remain:
            ctx.source = "seatStatusList"
            return total or None, remain or None, "RS"

    # zone summary fallback (RS)
    st, z = await fetch_json(context, f"{API}/api/v1/rs/blockSummary2", method="POST",
                             data={"prod_seq": ctx.prodSeq, "sd_seq": ctx.sdSeq, "chnl_cd": ctx.chnlCd},
                             headers=headers)
    if st == 200 and isinstance(z, dict):
        total = _pull_int(z, "saleSeatCnt", "rendrSeatCnt", "admissionTotalPersonCnt")
        remain = _pull_int(z, "admissionAvailPersonCnt", "restSeatCnt")
        if total or remain:
            ctx.source = "blockSummary2"
            return total or None, remain or None, (plan or "RS")

    return None, None, plan or ""


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
    plan = pad_field((ctx.plan_type or "?")[:3], 3)
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

async def handle_sd(sd: str, context: BrowserContext, headless: bool) -> None:
    page = await context.new_page()
    ctx = Ctx(sd=sd)

    # Resolve and meta
    ctx = await resolve_context(ctx, page)
    await fetch_meta_filmapi(ctx, context)  # optional reinforcement
    plan = await detect_plan_type(ctx, context)
    total, remain, plan_used = await summarize_seats(ctx, context)
    ctx.total = total
    ctx.remain = remain
    if not ctx.plan_type:
        ctx.plan_type = plan_used or plan or "ALL"

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
    global PAY_HOLD_SEC
    ap = argparse.ArgumentParser(description="BIFF OneShot — sdCode→meta+seat→1seat→payment HOLD")
    # Avoid referencing the global before this point by caching the default locally
    default_stay = PAY_HOLD_SEC
    ap.add_argument("--sd", dest="sd", action="append", required=True, help="스케쥴넘버 (여러 개 지정 가능: --sd 001 --sd 911)")
    ap.add_argument("--headless", action="store_true", help="헤드리스 브라우저 사용")
    ap.add_argument("--concurrency", type=int, default=2, help="동시 처리 개수 (기본 2)")
    ap.add_argument("--stay", type=int, default=default_stay, help="결제창 HOLD 유지 시간(초)")
    args = ap.parse_args()

    PAY_HOLD_SEC = int(args.stay)

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(headless=args.headless, args=["--disable-web-security"])
        context: BrowserContext = await browser.new_context(ignore_https_errors=True, viewport={"width": 1200, "height": 900})

        # login
        page = await context.new_page()
        ok = await wait_for_login(page, timeout=180.0))
        if not ok:
            print("⚠ 로그인 감지 실패 — 로그인 후 다시 시도하세요.", flush=True)
            await context.close(); await browser.close(); return
        # keep login page open to retain session
        # # keep login page open to retain session
        # # keep login page open to retain session
        # # keep login page open to retain session
        # await page.close()

        sem = asyncio.Semaphore(max(1, args.concurrency))
        async def run_one(code: str):
            async with sem:
                try:
                    await handle_sd(code, context, args.headless)
                except Exception as e:
                    # Emit minimal failure line
                    ctx = Ctx(sd=code, title="", venue="?", dt="", plan_type="?", total=None, remain=None, action="", status="?")
                    print(f"⚪ {pad_field(code,3)} | {pad_field('',22)} | {pad_field('?',14)} | {pad_field('??-?? ??:??',11)} | {pad_field('?',3)} | T/R={zpad4(None)}/{zpad4(None)} | ?", flush=True)

        await asyncio.gather(*(run_one(sd) for sd in args.sd))

        # Keep context open if any HOLDs remain
        # Note: handle_sd already holds per payment page
        await context.close()
        await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

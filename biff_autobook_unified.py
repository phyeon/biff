#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BIFF One‑Stop Auto Booker (Chrome, Playwright)

What it does (per sdCode hardcoded in this file):
  1) Open https://biff.maketicket.co.kr/ko/mypageLogin and sign in (ID/PW below or via env BIFF_ID/BIFF_PW)
  2) For each sdCode: navigate to booking page and harvest prodSeq/sdSeq/perfDate/csrfToken
  3) Call RS APIs to compute: title, date, venue/hall, total seats, remaining seats and seating plan type
  4) Log info. If remaining > 0:
        - NRS(자유석): pick 1 ticket type and proceed to payment page, then HOLD
        - ALL/ZONE(지정석): pick a seat as close to center as possible and proceed to payment page, then HOLD

Notes:
  • Uses Playwright with the real Chrome channel. Install: `pip install playwright` then `playwright install`.
  • Run: `python biff_autobook_unified.py --sd 001 --sd 002 --headless 0` (or edit SD_CODES below)
  • Tune SELECTOR_HINTS if the seat UI differs; this script tries multiple heuristics.
  • Environment overrides: BIFF_ID, BIFF_PW, HEADLESS, TIMEOUT_MS.

Disclaimer: This is best‑effort automation for BIFF One‑Stop; sites may change.
"""

import asyncio
import contextlib
import dataclasses
import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

# ======== USER CONFIG ========
SD_CODES: List[str] = [
    "001", "002",  # add more codes here
]
LANG = "ko"
BIFF_ID = os.getenv("BIFF_ID", "01036520261")
BIFF_PW = os.getenv("BIFF_PW", "040435")
HEADLESS = bool(int(os.getenv("HEADLESS", "1")))
TIMEOUT_MS = int(os.getenv("TIMEOUT_MS", "30000"))
HOLD_AT_PAYMENT = True  # keep the payment tab open (until user closes the script)

LOGIN_URL = "https://biff.maketicket.co.kr/ko/mypageLogin"
BOOKING_URL = "https://filmonestop.maketicket.co.kr/booking?sdCode={sd}"  
ONESTOP_RS_URL = "https://filmonestop.maketicket.co.kr/onestop/rs?prodSeq={prod}&sdSeq={sdseq}"
ONESTOP_ORIGIN = "https://filmonestop.maketicket.co.kr"
FILMAPI_BASE = "https://filmapi.maketicket.co.kr/api/v1"
RS_BASE     = "https://filmonestopapi.maketicket.co.kr/api/v1/rs"       

OVR_PROD = os.getenv("PROD_SEQ", "")
OVR_SDSEQ = os.getenv("SD_SEQ", "")
OVR_DATE = os.getenv("PERF_DATE", "")

# Heuristic selectors inside the oneStopFrame for seats/tickets/payment
SELECTOR_HINTS = {
    # A. Generic next/proceed/payment buttons
    "proceed_buttons": [
        "button:has-text('결제')",
        "button:has-text('다음')",
        "button:has-text('선택완료')",
        "a:has-text('결제')",
        "a:has-text('다음')",
        "a:has-text('선택완료')",
        ".btn_pay", ".btn_next", ".btn-submit", "#btnPay", "#btnNext",
    ],
    # B. Ticket type increment (for NRS)
    "ticket_plus": [
        ".btn-plus", "button.plus", "button[aria-label*='추가']",
        "button:has-text('+')",
    ],
    # B2. Ticket quantity select (alternative UI)
    "ticket_select": [
        "select", "select.qty", "select[name*='qty']", "select[name*='count']",
    ],
    # C. Seat elements (for ALL/ZONE). We will pick ones that look enabled/available.
    "seat_nodes": [
        "[data-status='ABLE']", "[data-sale='Y']", "[data-able='Y']",
        ".able", ".can", ".seat-able", ".seatCanUse",
        "svg circle.seat", "svg rect.seat", "canvas+div .seat",  # common widget patterns
    ],
}

@dataclasses.dataclass
class ShowCtx:
    sdCode: str
    prodSeq: str = ""
    sdSeq: str = ""
    perfDate: str = ""  # YYYYMMDD
    csrfToken: str = ""
    title: str = ""
    venue: str = ""
    hall: str = ""
    planTypeCd: str = ""  # ALL | ZONE | NRS
    total_seats: Optional[int] = None
    remain_seats: Optional[int] = None

# ================= HELPERS =================


from urllib.parse import unquote

async def csrf_from_any(ctx: BrowserContext, page: Page) -> str:
    # 1) meta / hidden input
    t = await _csrf_from_meta(page)
    if t:
        return t
    try:
        loc = page.locator("#sForm input[name='csrfToken'], input#csrfToken, input[name='csrfToken']")
        if await loc.count():
            v = await loc.first.input_value()
            if v:
                return v
    except Exception:
        pass
    # 2) oneStopFrame 안
    try:
        fr = await ensure_booking_iframe(page)
        if fr:
            loc = fr.locator("input[name='csrfToken'], #csrfToken")
            if await loc.count():
                v = await loc.first.input_value()
                if v:
                    return v
    except Exception:
        pass
    # 3) 쿠키의 XSRF-TOKEN (URL-decoded)
    try:
        cookies = await ctx.cookies(ONESTOP_ORIGIN)
        for c in cookies:
            if c.get("name") in ("XSRF-TOKEN", "CSRF-TOKEN", "X-CSRF-TOKEN"):
                val = unquote(c.get("value") or "")
                if val:
                    return val
    except Exception:
        pass
    return ""

def entry_url_candidates(prod_seq: str, sd_seq: str) -> List[str]:
    """Return likely working entry urls for the current build.
    Preference order: unified booking shell, then seat/price, then legacy with sdCode.
    """
    urls: List[str] = []
    if prod_seq and sd_seq:
        urls.append(f"{ONESTOP_ORIGIN}/booking?prodSeq={prod_seq}&sdSeq={sd_seq}")
    if prod_seq:
        urls.append(f"{ONESTOP_ORIGIN}/booking?prodSeq={prod_seq}")
        urls.append(f"{ONESTOP_ORIGIN}/seat?prodSeq={prod_seq}")
        urls.append(f"{ONESTOP_ORIGIN}/price?prodSeq={prod_seq}")
    # very last resort: sdCode route
    urls.append(BOOKING_URL.format(sd="{sd}"))  # placeholder; will .format later
    return urls

# ---- Network logger (prints key requests) ----
async def attach_netlogger(ctx: BrowserContext) -> None:
    def _on_response(resp):
        try:
            url = resp.url
            host_hit = (
                "filmonestopapi.maketicket.co.kr" in url or
                "filmapi.maketicket.co.kr" in url or
                "/rs/" in url or
                "/booking" in url
            )
            if not host_hit:
                return
            status = resp.status
            method = resp.request.method
            short = url.split("?")[0]
            print(f"↪ {status} {method} {short}")
        except Exception:
            pass
    ctx.on("response", _on_response)


def mask(s: str, show: int = 3) -> str:
    s = s or ""
    return s[:show] + "*" * max(0, len(s) - show)

async def _csrf_from_meta(p: Page) -> str:
    """Try to extract CSRF from meta tags or hidden input on top page."""
    try:
        token = await p.evaluate(
            "() => (document.querySelector('meta[name=_csrf]')?.content || "
            "document.querySelector('meta[name=csrf-token]')?.content || "
            "document.querySelector('meta[name=\"X-CSRF-TOKEN\"]')?.content || "
            "document.querySelector('#csrfToken')?.value || '')"
        )
        return token or ""
    except Exception:
        return ""

async def ensure_booking_iframe(page: Page):
    """Return the oneStopFrame (Frame object) if present, else None."""
    try:
        fr = page.frame(name="oneStopFrame")
        if fr:
            return fr
    except Exception:
        pass
    try:
        await page.wait_for_selector("#oneStopFrame", timeout=TIMEOUT_MS)
        return page.frame(name="oneStopFrame")
    except Exception:
        return None

async def harvest_booking_ctx(page: Page) -> Dict[str, str]:
    """Read hidden inputs on top document (#sForm) and, if empty, inside oneStopFrame.
    It also checks by input[name=...] as some builds omit IDs."""
    keys = [
        "prodSeq", "sdSeq", "perfDate", "sdCode", "csrfToken", "perfMainName",
        "planTypeCd", "prodTyCd", "saleTycd", "saleCondNo",
        "sdStartDt", "sdStartHour", "venueNm", "hallNm",
    ]
    out: Dict[str, str] = {}

    async def _harvest_from(scope):
        for k in keys:
            for sel in (f"#{k}", f"input[name='{k}']"):
                try:
                    loc = scope.locator(sel)
                    if await loc.count():
                        out.setdefault(k, await loc.first.input_value())
                        break
                except Exception:
                    pass

    await _harvest_from(page)
    if not out.get("prodSeq") or not out.get("sdSeq"):
        fr = await ensure_booking_iframe(page)
        if fr:
            await _harvest_from(fr)
    return out

async def map_sd_from_filmapi(ctx: BrowserContext, sd: str) -> Dict[str, Any]:
    """
    sdCode -> {prodSeq, sdSeq, perfDate, ...}를 예매 페이지에서 '직접' 수확.
    - /ko 금지: 반드시 /booking?sdCode= 로 진입 (쿼리 유실 감지)
    - 상단 #sForm, iframe, meta, window 전역(JSON blob)까지 훑기
    - 쿼리 유실되면 외부 카탈로그 API로 매핑 재시도(헤더 보강)
    """
    def _lost_query(u: str) -> bool:
        try:
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(u).query or "")
            return not (qs.get("sdCode") or qs.get("prodSeq") or qs.get("sdSeq"))
        except Exception:
            return True

    page = await ctx.new_page()
    got: Dict[str, Any] = {}
    try:
        url = f"{ONESTOP_ORIGIN}/booking?sdCode={sd}"  # << /ko 금지
        await page.goto(url, timeout=TIMEOUT_MS)
        await page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)

        # 쿼리 유실 감지 (001/002 처럼 유효하지 않으면 종종 /booking으로 날아감)
        if _lost_query(page.url):
            print(f"⚠️  [{sd}] sdCode 쿼리가 서버에서 제거됨 → 외부 카탈로그로 매핑 시도")

        # 1) 상단/iframe/hidden input 수확
        got.update(await harvest_booking_ctx(page))
        if not got.get("csrfToken"):
            got["csrfToken"] = await _csrf_from_meta(page)

        # 2) 페이지 정규식 수확 (보강)
        html = await page.content()
        for pat, key in [
            (r'prodSeq["\']?\s*[:=]\s*["\']?(\d+)', "prodSeq"),
            (r'sdSeq["\']?\s*[:=]\s*["\']?(\d+)',   "sdSeq"),
            (r'perfDate["\']?\s*[:=]\s*["\']?(\d{8})', "perfDate"),
            (r'sdCode["\']?\s*[:=]\s*["\']?(\d+)', "sdCode"),
            (r'perfMainName["\']?\s*[:=]\s*["\']?([^"\']]+)', "perfMainName"),
            (r'venueNm["\']?\s*[:=]\s*["\']?([^"\']]+)', "venueNm"),
            (r'hallNm["\']?\s*[:=]\s*["\']?([^"\']]+)', "hallNm"),
            (r'planTypeCd["\']?\s*[:=]\s*["\']?([A-Z]+)', "planTypeCd"),
        ]:
            m = re.search(pat, html)
            if m and not got.get(key):
                got[key] = m.group(1)

        # 3) window 전역(JSON blob) 수확
        with contextlib.suppress(Exception):
            state = await page.evaluate("() => (window.__INITIAL_STATE__ || window.__NUXT__ || window.__APP_STATE__ || null)")
            if isinstance(state, dict):
                def pick(d, k): return d.get(k) if isinstance(d, dict) else None
                for d in [state, pick(state, "data") or {}, pick(state, "pageProps") or {}]:
                    for k in ("prodSeq","sdSeq","perfDate","sdCode","perfMainName","venueNm","hallNm","planTypeCd"):
                        if not got.get(k) and isinstance(d, dict) and d.get(k):
                            got[k] = str(d.get(k))

        # 최소 필수 체크
        if str(got.get("prodSeq") or "") and str(got.get("sdSeq") or ""):
            return got

        # === 외부 카탈로그 매핑(헤더 보강) ===
        print(f"↪ [{sd}] 카탈로그 API 매핑 시도")
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko,en;q=0.9",
            "Origin": ONESTOP_ORIGIN,
            "Referer": f"{ONESTOP_ORIGIN}/booking",
            "User-Agent": "Mozilla/5.0",
        }
        # 후보 엔드포인트들 (운영 배포마다 경로 다를 수 있어 순차 시도)
        catalog_urls = [
            # BIFF 회차에 맞게 운영되는 전시(상영) 리스트
            "https://filmonestopapi.maketicket.co.kr/api/prod/prods/biff/29/exhs",
            "https://filmonestopapi.maketicket.co.kr/api/v1/prod/prods/biff/29/exhs",
            # 구형
            f"{FILMAPI_BASE}/prodList?sdCode={sd}",
        ]
        for cu in catalog_urls:
            try:
                res = await ctx.request.get(cu, headers=headers, timeout=15000)
                if not res.ok:
                    continue
                j = await res.json()
                lst = []
                if isinstance(j, dict):
                    lst = j.get("data", {}).get("list", []) or j.get("list") or j.get("data") or []
                elif isinstance(j, list):
                    lst = j
                # sdCode 필드가 3자리/정수 등 다양할 수 있음 → 문자열 비교 유연화
                target = str(sd)
                def norm(x): return str(x).strip().lstrip("0") or "0"
                for it in lst:
                    sc = str(it.get("sdCode") or it.get("sdcode") or it.get("sd") or "")
                    if sc and (sc == target or norm(sc) == norm(target)):
                        it["perfMainNm"] = it.get("prodNm") or it.get("perfNm") or it.get("title") or ""
                        # sdSeq 기본값 보정
                        if not it.get("sdSeq"):
                            it["sdSeq"] = it.get("sdseq") or it.get("sessionSeq") or "1"
                        return it
            except Exception as e:
                print(f"[{sd}] 카탈로그 응답 처리 실패: {e}")
    except Exception as e:
        print(f"[{sd}] 페이지 수확 실패: {e}")
    finally:
        with contextlib.suppress(Exception):
            await page.close()
    return {}



async def rs_post(ctx: BrowserContext, booking_url: str, csrf: str, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Origin": ONESTOP_ORIGIN,
        "Referer": booking_url,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    }
    if csrf:
        headers["X-CSRF-TOKEN"] = csrf

    url = f"{RS_BASE}/{path.lstrip('/')}"
    payload = {**data}

    # 👇 필수 파라미터 보강
    if "lang" not in payload:
        payload["lang"] = LANG  # "ko"
    if csrf and "csrfToken" not in payload:
        payload["csrfToken"] = csrf

    r = await ctx.request.post(url, data=payload, headers=headers)
    if not r.ok:
        return {"__error__": f"HTTP {r.status}", "__url__": url}
    try:
        return await r.json()
    except Exception:
        return {"__error__": "Invalid JSON", "__url__": url}

def _unwrap_data(obj: Any) -> Any:
    if isinstance(obj, dict) and isinstance(obj.get("data"), (dict, list)):
        return obj["data"]
    return obj


async def compute_counts(ctx: BrowserContext, booking_url: str, sc: ShowCtx) -> Tuple[Optional[int], Optional[int]]:
    base_args = {
        "prodSeq": sc.prodSeq,
        "sdSeq": sc.sdSeq,
        "chnlCd": "WEB",
        "perfDate": sc.perfDate or "",
        "saleTycd": "SALE_NORMAL",
        "saleCondNo": "1",
        "lang": LANG,  # 👈 필수
    }
    total = None
    remain = None

    psum = await rs_post(ctx, booking_url, sc.csrfToken, "prodSummary", base_args)
    bsum = await rs_post(ctx, booking_url, sc.csrfToken, "blockSummary2", base_args)

    # 👇 래퍼 해제
    psum = _unwrap_data(psum)
    bsum = _unwrap_data(bsum)

    # prodSummary variants
    if isinstance(psum, dict):
        try:
            total = (psum.get("totalSeatCnt") or psum.get("totalSeat") or psum.get("total"))
            remain = (psum.get("remainSeatCnt") or psum.get("noneSeatCnt") or psum.get("remain"))
            if isinstance(total, str): total = int(total or 0)
            if isinstance(remain, str): remain = int(remain or 0)
        except Exception:
            pass

    # blockSummary2 (list or dict.blocks)
    if (total is None or remain is None) and isinstance(bsum, (dict, list)):
        blocks = []
        if isinstance(bsum, dict):
            blocks = bsum.get("blocks") or bsum.get("list") or []
        else:
            blocks = bsum
        t = r = 0
        for b in blocks:
            try:
                t += int(b.get("totalSeatCnt") or b.get("totalSeat") or b.get("total") or 0)
                r += int(b.get("remainSeatCnt") or b.get("remainSeat") or b.get("remain") or 0)
            except Exception:
                pass
        if t:
            total, remain = t, r

    # Seat-level (ALL/ZONE) fallback
    if (sc.planTypeCd in {"ALL", "ZONE"} or sc.planTypeCd == "") and (total is None or remain is None):
        base = await rs_post(ctx, booking_url, sc.csrfToken, "seatBaseMap", base_args)
        stat = await rs_post(ctx, booking_url, sc.csrfToken, "seatStatusList", base_args)
        base = _unwrap_data(base)
        stat = _unwrap_data(stat)

        seats = []
        statuses = []
        try:
            seats = base.get("seats") or base.get("list") or []
        except Exception:
            pass
        try:
            statuses = stat.get("statuses") or stat.get("list") or []
        except Exception:
            pass

        if seats:
            total = len(seats)
        if statuses:
            def is_ok(s: Dict[str, Any]) -> bool:
                v = str(s.get("saleStatus") or s.get("status") or s.get("able") or s.get("sale") or "").upper()
                return v in {"Y","ABLE","CAN","OK","EMPTY","TRUE"}
            remain = sum(1 for s in statuses if is_ok(s))

    return total, remain


async def pick_and_proceed_ui(frame, plan: str, outer_page: Page) -> bool:
    """Pick 1 ticket/seat and try to proceed to payment.
    Works with a Frame or Page-like object (must support locator/evaluate)."""
    # NRS: increase ticket count
    if plan == "NRS":
        # Try + button first
        clicked = False
        for sel in SELECTOR_HINTS["ticket_plus"]:
            try:
                el = frame.locator(sel)
                if await el.count():
                    await el.first.click(timeout=4000)
                    clicked = True
                    break
            except Exception:
                pass
        if not clicked:
            # Try selects: set first numeric <select> to 1
            for sel in SELECTOR_HINTS.get("ticket_select", []):
                try:
                    el = frame.locator(sel)
                    n = await el.count()
                    for i in range(min(n, 5)):
                        opt_vals = await el.nth(i).evaluate("(s)=>Array.from(s.options).map(o=>o.value)")
                        if any(v == "1" for v in opt_vals):
                            await el.nth(i).select_option("1")
                            clicked = True
                            break
                    if clicked:
                        break
                except Exception:
                    pass
    else:
        # Choose center-ish seat
        seats: List[Tuple[float, Any]] = []
        for sel in SELECTOR_HINTS["seat_nodes"]:
            try:
                loc = frame.locator(sel)
                n = await loc.count()
                if n == 0:
                    continue
                box0 = await frame.evaluate("() => ({w: window.innerWidth, h: window.innerHeight})")
                cx, cy = box0.get("w", 1000) / 2, box0.get("h", 800) / 2
                n = min(n, 200)
                for i in range(n):
                    b = await loc.nth(i).bounding_box()
                    if not b:
                        continue
                    dx = (b["x"] + b["width"] / 2) - cx
                    dy = (b["y"] + b["height"] / 2) - cy
                    seats.append((dx * dx + dy * dy, loc.nth(i)))
            except Exception:
                pass
        if seats:
            seats.sort(key=lambda x: x[0])
            with contextlib.suppress(Exception):
                await seats[0][1].click(timeout=5000, force=True)

    # Click proceed
    for sel in SELECTOR_HINTS["proceed_buttons"]:
        try:
            el = frame.locator(sel)
            if await el.count():
                await el.first.click(timeout=5000)
                await asyncio.sleep(0.8)
        except Exception:
            continue

    # Payment detection in outer page URL
    try:
        if "/payment" in (outer_page.url or ""):
            return True
    except Exception:
        pass
    # Or inside iframe
    try:
        href = await frame.evaluate("() => location.href")
        if "/payment" in href:
            return True
    except Exception:
        pass
    return False

async def login_and_get_page(ctx: BrowserContext) -> Page:
    """Robust login that tries multiple login URLs and validates success.
    Prints explicit errors if redirected to error/404 page."""
    login_urls = [
        "https://biff.maketicket.co.kr/ko/mypageLogin",
        "https://biff.maketicket.co.kr/ko/login",
        "https://biff.maketicket.co.kr/login",
    ]

    async def _is_logged_in(p: Page) -> bool:
        try:
            if await p.locator("a:has-text('로그아웃'), a[href*='logout'], .logout").count():
                return True
            if await p.locator("a:has-text('마이페이지'), a[href*='mypage']").count():
                return True
        except Exception:
            pass
        return False

    page = await ctx.new_page()
    for i, url in enumerate(login_urls):
        try:
            print(f"🔑 로그인 페이지 시도 {i+1}/{len(login_urls)} → {url}")
            await page.goto(url, timeout=TIMEOUT_MS)
            await page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)

            # Fill ID/PW if form exists
            id_filled = False
            for css in [
                "#id", "#loginId", "input[name=loginId]", "input[name=id]",
                "#email", "input[name=email]", "input[type=email]", "input[type=tel]",
            ]:
                try:
                    if await page.locator(css).count():
                        await page.locator(css).first.fill(BIFF_ID, timeout=TIMEOUT_MS)
                        id_filled = True
                        break
                except Exception:
                    pass

            pw_filled = False
            for css in ["#pw", "#password", "input[name=password]", "#userPw", "input[name=pw]", "input[type=password]"]:
                try:
                    if await page.locator(css).count():
                        await page.locator(css).first.fill(BIFF_PW, timeout=TIMEOUT_MS)
                        pw_filled = True
                        break
                except Exception:
                    pass

            clicked = False
            for sel in ["button:has-text('로그인')", "#btnLogin", ".btn_login", "button[type=submit]", "input[type=submit]"]:
                try:
                    if await page.locator(sel).count():
                        await page.locator(sel).first.click(timeout=TIMEOUT_MS)
                        clicked = True
                        break
                except Exception:
                    pass

            with contextlib.suppress(Exception):
                await page.wait_for_load_state("networkidle", timeout=TIMEOUT_MS)

            # Detect explicit error page (like the screenshot)
            try:
                if await page.locator("text=요청하신 페이지를 찾을 수 없습니다").count():
                    print("⛔ 로그인 후 에러 페이지로 리다이렉트됨 — URL:", page.url)
            except Exception:
                pass

            if await _is_logged_in(page):
                print("✅ 로그인 성공")
                break
            else:
                if not id_filled or not pw_filled or not clicked:
                    print("⚠️ 로그인 폼 감지 실패 또는 클릭 실패 — 다음 후보 URL 시도")
                else:
                    print("⚠️ 로그인 시도 후에도 미로그인 상태 — 다음 후보 URL 시도")
        except Exception as e:
            print(f"⚠️ 로그인 시도 중 예외({url}): {e}")
            continue

    # SSO bridge toward onestop domain to propagate session
    try:
        warm = await ctx.new_page()
        await warm.goto(f"{ONESTOP_ORIGIN}/", timeout=TIMEOUT_MS)
        with contextlib.suppress(Exception):
            await warm.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)
        await warm.close()
    except Exception:
        pass

    return page

async def process_show(ctx: BrowserContext, sd: str) -> None:
    page = await ctx.new_page()
    print(f"\n🎬 [ {sd} ] 진입 준비")

    top: Dict[str, Any] = {}

    # 1) 오버라이드가 있으면 매핑/수확 생략
    if OVR_PROD and OVR_SDSEQ:
        top["prodSeq"] = OVR_PROD
        top["sdSeq"] = OVR_SDSEQ
        if OVR_DATE:
            top["perfDate"] = OVR_DATE
    else:
        # 2) sdCode 기반 매핑/수확
        cand = await map_sd_from_filmapi(ctx, sd)
        if cand:
            top["prodSeq"] = str(cand.get("prodSeq") or cand.get("prodseq") or "")
            top["sdSeq"] = str(cand.get("sdSeq") or cand.get("sdseq") or "")
            top["perfMainName"] = cand.get("perfMainNm") or cand.get("perfNm") or ""
            top["venueNm"] = cand.get("venueNm") or cand.get("venue") or ""
            top["hallNm"] = cand.get("hallNm") or cand.get("hall") or ""
            dt = (cand.get("sdDate") or cand.get("perfDate") or "").replace(".", "").replace("-", "")
            if dt:
                top["perfDate"] = dt

    # 3) 컨텍스트 구성
    sc = ShowCtx(
        sdCode=sd,
        prodSeq=str(top.get("prodSeq") or ""),
        sdSeq=str(top.get("sdSeq") or ""),
        perfDate=str(top.get("perfDate") or ""),
        csrfToken="",
        title=top.get("perfMainName") or "",
        venue=top.get("venueNm") or "",
        hall=top.get("hallNm") or "",
        planTypeCd=top.get("planTypeCd") or "",
    )

    # 4) 엔트리 URL 시도 (prod/sdSeq 우선)
    urls: List[str] = []
    if sc.prodSeq and sc.sdSeq:
        urls.append(f"{ONESTOP_ORIGIN}/booking?prodSeq={sc.prodSeq}&sdSeq={sc.sdSeq}")
    urls.extend([u.format(sd=sd) for u in entry_url_candidates(sc.prodSeq, sc.sdSeq)])

    frame = None
    booking_url = None
    for u in urls:
        try:
            await page.goto(u, timeout=TIMEOUT_MS)
            await page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)
            frame = await ensure_booking_iframe(page)
            price_ui = (await page.locator("text=가격선택").count()) > 0
            # 쿼리가 날아가도 referer 확보만 되면 충분
            if frame or price_ui or True:
                booking_url = page.url or f"{ONESTOP_ORIGIN}/booking"
                break
        except Exception:
            continue

    if not booking_url:
        fallback = BOOKING_URL.format(sd=sd)
        print(f"   ↪ fallback 진입: {fallback}")
        with contextlib.suppress(Exception):
            await page.goto(fallback, timeout=TIMEOUT_MS)
            await page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)
        frame = await ensure_booking_iframe(page)
        booking_url = page.url or f"{ONESTOP_ORIGIN}/booking"

    # 5) onestop/rs 워밍업 → 다시 booking(레퍼러 확보)
    if sc.prodSeq and sc.sdSeq:
        with contextlib.suppress(Exception):
            warm_url = ONESTOP_RS_URL.format(prod=sc.prodSeq, sdseq=sc.sdSeq)
            await page.goto(warm_url, timeout=TIMEOUT_MS)
            await page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)
            await page.goto(f"{ONESTOP_ORIGIN}/booking?prodSeq={sc.prodSeq}&sdSeq={sc.sdSeq}", timeout=TIMEOUT_MS)
            await page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)

    # 6) CSRF 토큰 확보 (메타/hidden/iframe/쿠키 모두 시도)
    sc.csrfToken = await csrf_from_any(ctx, page)

    # 보조 수확으로 누락된 값 보강
    if not sc.prodSeq or not sc.sdSeq or not sc.perfDate:
        got = await harvest_booking_ctx(page)
        sc.prodSeq = sc.prodSeq or str(got.get("prodSeq") or "")
        sc.sdSeq = sc.sdSeq or str(got.get("sdSeq") or "")
        sc.perfDate = sc.perfDate or str(got.get("perfDate") or "")
        sc.title = sc.title or (got.get("perfMainName") or "")
        sc.venue = sc.venue or (got.get("venueNm") or "")
        sc.hall = sc.hall or (got.get("hallNm") or "")
        sc.planTypeCd = sc.planTypeCd or (got.get("planTypeCd") or "")

    # 디버그
    print(f"DBG RS ctx → prodSeq={sc.prodSeq or '-'}, sdSeq={sc.sdSeq or '-'}, perfDate={sc.perfDate or '-'}, csrf={'Y' if sc.csrfToken else 'N'}")

    # 7) RS: prod → 정보 보강
    j = await rs_post(ctx, booking_url, sc.csrfToken, "prod", {
        "prodSeq": sc.prodSeq, "sdSeq": sc.sdSeq, "chnlCd": "WEB",
        "saleTycd": "SALE_NORMAL", "saleCondNo": "1",
        "perfDate": sc.perfDate, "lang": LANG,
    })
    if isinstance(j, dict) and "__error__" not in j:
        with contextlib.suppress(Exception):
            inf = j.get("prodInform") or j.get("prod") or {}
            sc.planTypeCd = inf.get("planTypeCd") or inf.get("planType") or sc.planTypeCd
            sc.title = sc.title or (inf.get("perfMainName") or inf.get("perfNm") or "")
            sc.venue = sc.venue or (inf.get("venueNm") or "")
            sc.hall = sc.hall or (inf.get("hallNm") or "")
            sc.perfDate = sc.perfDate or (inf.get("sdStartDt") or inf.get("perfDate") or "")

    # 8) 좌석 합계/잔여
    sc.total_seats, sc.remain_seats = await compute_counts(ctx, booking_url, sc)

    # 출력/진행
    pdate = sc.perfDate
    if pdate and len(pdate) >= 8:
        pdate = f"{pdate[:4]}-{pdate[4:6]}-{pdate[6:8]}"
    mode = {"NRS": "자유석", "ALL": "지정석(좌석맵)", "ZONE": "지정석(구역)"}.get(sc.planTypeCd, sc.planTypeCd or "?")
    total = sc.total_seats if sc.total_seats is not None else "?"
    remain = sc.remain_seats if sc.remain_seats is not None else "?"
    print(f"ℹ️  [{sd}] {sc.title or '(제목미상)'} | {sc.venue} {sc.hall} | {pdate} | 총={total} 잔여={remain} | 모드={mode}")

    if sc.remain_seats is not None and sc.remain_seats <= 0:
        print(f"⏸️  [{sd}] 잔여석 없음 — 스킵")
        await page.close()
        return

    container = frame or page
    plan_guess = sc.planTypeCd or ("NRS" if (await page.locator("text=가격선택").count()) else "ALL")
    ok = await pick_and_proceed_ui(container, plan_guess, page)
    if ok:
        print(f"✅ [{sd}] 결제 단계 진입 시도")
    else:
        print(f"⚠️  [{sd}] 결제 단계 진입 확인 불가 (사이트 UI 변경 가능) — 창을 유지합니다")

    if HOLD_AT_PAYMENT:
        print("[HOLD] 결제창/예약 과정을 확인한 뒤 창을 닫으세요. (Ctrl+C로 종료)")
    else:
        await page.close()


async def launch_browser(pw, headless: bool) -> Browser:
    """Robust Chrome launcher that handles Chrome's new headless mode.
    Tries channel=chrome with --headless=new, then falls back to bundled Chromium,
    then finally to headful Chrome if needed."""
    launch_kwargs = {"channel": "chrome", "headless": headless}
    if headless:
        launch_kwargs["args"] = ["--headless=new"]
    try:
        return await pw.chromium.launch(**launch_kwargs)
    except Exception:
        # Fallback 1: bundled Chromium with new headless flag
        try:
            return await pw.chromium.launch(
                headless=headless, args=(["--headless=new"] if headless else [])
            )
        except Exception as e2:
            # Fallback 2: headful Chrome
            if headless:
                with contextlib.suppress(Exception):
                    return await pw.chromium.launch(channel="chrome", headless=False)
            raise e2

async def main() -> None:
    # ▼ 추가: 전역 오버라이드 변수 사용을 명시
    global OVR_PROD, OVR_SDSEQ, OVR_DATE

    sds: List[str] = []
    headless = HEADLESS
    argv = sys.argv[1:]
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--sd" and i + 1 < len(argv):
            sds.append(argv[i + 1])
            i += 2
            continue
        if a == "--headless" and i + 1 < len(argv):
            headless = bool(int(argv[i + 1]))
            i += 2
            continue
        # ▼ 중요: i += 1 전에 오버라이드 3종을 처리해야 함
        if a == "--prodSeq" and i + 1 < len(argv):
            OVR_PROD = argv[i + 1]
            i += 2
            continue
        if a == "--sdSeq" and i + 1 < len(argv):
            OVR_SDSEQ = argv[i + 1]
            i += 2
            continue
        if a == "--perfDate" and i + 1 < len(argv):
            OVR_DATE = argv[i + 1]
            i += 2
            continue
        # ▼ 아무 인자에도 해당 안 될 때만 1칸 전진
        i += 1

    if not sds:
        sds = SD_CODES[:]

    # (선택) 디버그 출력: 오버라이드가 실제 반영됐는지 확인
    print(f"OVR => prodSeq={OVR_PROD or '-'} sdSeq={OVR_SDSEQ or '-'} perfDate={OVR_DATE or '-'}")

    # Launch Chrome
    async with async_playwright() as pw:
        browser: Browser = await launch_browser(pw, headless)
        ctx: BrowserContext = await browser.new_context(locale="ko-KR")
        await attach_netlogger(ctx)  # 네트워크 로그 출력 활성화
        ctx.set_default_timeout(TIMEOUT_MS)

        # Login first
        page = await login_and_get_page(ctx)
        print(f"🔐 [로그인 시도] id={mask(BIFF_ID)} pw=******** (후속 로그로 성공/실패 표시)")
        with contextlib.suppress(Exception):
            await page.wait_for_load_state("networkidle")
        await page.close()

        # Process each schedule code
        for sd in sds:
            try:
                await process_show(ctx, sd)
            except Exception as e:
                print(f"❌ [{sd}] 처리 실패: {e}")

        if HOLD_AT_PAYMENT:
            print("\n[RUN] 브라우저를 유지합니다. 창을 닫거나 Ctrl+C 로 종료하세요…")
            while True:
                await asyncio.sleep(1)
        else:
            await ctx.close()
            await browser.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

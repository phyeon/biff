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
BOOKING_URL = "https://filmonestop.maketicket.co.kr/ko/booking?sdCode={sd}"
ONESTOP_RS_URL = "https://filmonestop.maketicket.co.kr/ko/onestop/rs?prodSeq={prod}&sdSeq={sdseq}"
ONESTOP_ORIGIN = "https://filmonestop.maketicket.co.kr"
FILMAPI_BASE = "https://filmapi.maketicket.co.kr/api/v1"
RS_BASE = "https://filmonestopapi.maketicket.co.kr/api/v1/rs"

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

# 아래 코드로 기존 map_sd_from_filmapi 함수 전체를 교체하세요.

async def map_sd_from_filmapi(ctx: BrowserContext, sd: str) -> Dict[str, Any]:
    """
    현재 작동하는 API에서 모든 영화 정보를 가져온 후,
    필요한 sdCode에 해당하는 영화 정보만 찾아서 반환합니다.
    """
    # 2025년 기준 29회 BIFF. 이 숫자는 매년 바뀔 수 있습니다.
    event_id = 29
    url = f"https://filmonestopapi.maketicket.co.kr/api/prod/prods/biff/{event_id}/exhs"
    try:
        r = await ctx.request.get(url, timeout=15000)
        if r.ok:
            j = await r.json()
            show_list = j.get("data", {}).get("list", [])
            for show in show_list:
                if str(show.get("sdCode")) == sd:
                    # 기존 코드와 형식을 맞추기 위해 키 이름을 일부 변경
                    show["perfMainNm"] = show.get("prodNm")
                    return show # 일치하는 영화 정보를 찾으면 바로 반환
    except Exception as e:
        print(f"[{sd}] 영화 정보를 API에서 가져오는 중 오류 발생: {e}")
    
    return {} # 영화 정보를 찾지 못하면 빈 딕셔너리 반환

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

    # Resolve mapping first (don't rely on sdCode route)
    cand = await map_sd_from_filmapi(ctx, sd)
    top: Dict[str, Any] = {}
    if cand:
        top["prodSeq"] = str(cand.get("prodSeq") or cand.get("prodseq") or "")
        top["sdSeq"] = str(cand.get("sdSeq") or cand.get("sdseq") or "")
        top["perfMainName"] = cand.get("perfMainNm") or cand.get("perfNm") or ""
        top["venueNm"] = cand.get("venueNm") or cand.get("venue") or ""
        top["hallNm"] = cand.get("hallNm") or cand.get("hall") or ""
        dt = (cand.get("sdDate") or cand.get("perfDate") or "").replace(".", "").replace("-", "")
        if dt:
            top["perfDate"] = dt

    # mapping(top) 끝난 직후, sc = ShowCtx(...) 만들기 전에 ↓↓↓
    if OVR_PROD:
        top["prodSeq"] = OVR_PROD
    if OVR_SDSEQ:
        top["sdSeq"] = OVR_SDSEQ
    if OVR_DATE:
        top["perfDate"] = OVR_DATE

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

    # Try multiple entry urls (favor /booking?prodSeq=...)
    urls = [u.format(sd=sd) for u in entry_url_candidates(sc.prodSeq, sc.sdSeq)]
    frame = None
    booking_url = None
    for u in urls:
        try:
            await page.goto(u, timeout=TIMEOUT_MS)
            await page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)
            # consider loaded if frame exists or price UI text appears
            frame = await ensure_booking_iframe(page)
            price_ui = (await page.locator("text=가격선택").count()) > 0
            if frame or price_ui:
                booking_url = page.url
                break
        except Exception:
            continue

    if not booking_url:
        fallback = BOOKING_URL.format(sd=sd)
        print(f"   ↪ fallback 진입: {fallback}")
        try:
            await page.goto(fallback, timeout=TIMEOUT_MS)
        except Exception:
            # 최후의 참조자(Referer) 채우기
            await page.goto(f"{ONESTOP_ORIGIN}/booking", timeout=TIMEOUT_MS)
        frame = await ensure_booking_iframe(page)
        booking_url = page.url or f"{ONESTOP_ORIGIN}/booking"

    # CSRF and context
    sc.csrfToken = await _csrf_from_meta(page)
    if not sc.prodSeq or not sc.sdSeq:
        # Try harvest from page or iframe
        got = await harvest_booking_ctx(page)
        sc.prodSeq = sc.prodSeq or str(got.get("prodSeq") or "")
        sc.sdSeq = sc.sdSeq or str(got.get("sdSeq") or "")
        sc.perfDate = sc.perfDate or str(got.get("perfDate") or "")
        sc.title = sc.title or (got.get("perfMainName") or "")
        sc.venue = sc.venue or (got.get("venueNm") or "")
        sc.hall = sc.hall or (got.get("hallNm") or "")
        sc.planTypeCd = sc.planTypeCd or (got.get("planTypeCd") or "")

    # Normalize via prod API
    j = await rs_post(ctx, booking_url, sc.csrfToken, "prod", {
        "prodSeq": sc.prodSeq, "sdSeq": sc.sdSeq, "chnlCd": "WEB",
        "saleTycd": "SALE_NORMAL", "saleCondNo": "1",
        "perfDate": sc.perfDate, "lang": LANG,   # ← perfDate 중복 제거
    })
    if isinstance(j, dict) and "__error__" not in j:
        with contextlib.suppress(Exception):
            inf = j.get("prodInform") or j.get("prod") or {}
            sc.planTypeCd = inf.get("planTypeCd") or inf.get("planType") or sc.planTypeCd
            sc.title = sc.title or (inf.get("perfMainName") or inf.get("perfNm") or "")
            sc.venue = sc.venue or (inf.get("venueNm") or "")
            sc.hall = sc.hall or (inf.get("hallNm") or "")
            sc.perfDate = sc.perfDate or (inf.get("sdStartDt") or inf.get("perfDate") or "")

    # Compute counts
    sc.total_seats, sc.remain_seats = await compute_counts(ctx, booking_url, sc)

    # Pretty date
    pdate = sc.perfDate
    if pdate and len(pdate) >= 8:
        pdate = f"{pdate[:4]}-{pdate[4:6]}-{pdate[6:8]}"

    mode = {
        "NRS": "자유석",
        "ALL": "지정석(좌석맵)",
        "ZONE": "지정석(구역)",
    }.get(sc.planTypeCd, sc.planTypeCd or "?")

    total = sc.total_seats if sc.total_seats is not None else "?"
    remain = sc.remain_seats if sc.remain_seats is not None else "?"

    print(f"ℹ️  [{sd}] {sc.title or '(제목미상)'} | {sc.venue} {sc.hall} | {pdate} | 총={total} 잔여={remain} | 모드={mode}")

    if sc.remain_seats is not None and sc.remain_seats <= 0:
        print(f"⏸️  [{sd}] 잔여석 없음 — 스킵")
        await page.close()
        return

    # Auto-pick: allow both iframe and top-level
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
    # Parse CLI args (simple)
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
        i += 1
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
    if not sds:
        sds = SD_CODES[:]

    # Launch Chrome
    async with async_playwright() as pw:
        browser: Browser = await launch_browser(pw, headless)
        ctx: BrowserContext = await browser.new_context()
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

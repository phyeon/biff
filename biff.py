# -*- coding: utf-8 -*-
# BIFF One-Click v5
# - 로그인 → 모든 sdCode 동시 처리
# - 좌석맵: 존/좌석 맵 API로 전체/잔여 좌석 계산 로그 + 좌석 1개 선택 → Next
# - 자유석: seatStateInfo → prodChk → tickettype → prodlimit → pricelimit 연쇄 호출 후 Next
# - 결제(주문) 단계 도달하면 성공 처리
# 사용: python biff_oneclick_concurrent_v5.py

import re, asyncio, urllib.parse, json, random
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple, Union
from playwright.async_api import async_playwright, Page, Frame
import time

# === TRACE: env & paths ===
import os, uuid, datetime, pathlib
TRACE_ENABLE = os.getenv("TRACE_ENABLE", "1") == "1"
TRACE_DIR = pathlib.Path(os.getenv("TRACE_DIR", "./debug"))
TRACE_DIR.mkdir(parents=True, exist_ok=True)
TRACE_WIRE_BODY = os.getenv("TRACE_WIRE_BODY", "0") == "1"  # POST body 저장 여부(민감정보 주의)
PAY_STAY = bool(int(os.getenv("PAY_STAY", "1")))   # 1=결제에서 멈춤(기본), 0=자동종료
PAY_STAY_TIMEOUT_MS = int(os.getenv("PAY_STAY_TIMEOUT_MS", "0"))  # 0=무한
# ▼ 하드코딩 회차 코드
SD_CODES = ["001", "002", "554", "910", "324", "911"]
MAX_CONCURRENCY = len(SD_CODES)
AVAILABLE_CODES = {"SS01000", "SS02000", "SS03000"}
BASE_RESMAIN = "https://biff.maketicket.co.kr/ko/resMain?sdCode={sd}"
LOGIN_URL    = "https://biff.maketicket.co.kr/ko/login"

OPEN_TIMEOUT  = 25_000
CLICK_TIMEOUT = 10_000
STEP_TIMEOUT  = 60_000
PAYMENT_DETECT_TIMEOUT = 15_000
PAYMENT_HINT_RX = re.compile(
    r"(pay|payment|order|listSaleCupnByPay|listSaleAdvtkByPay|delivery|pricelimit)",
    re.I
)
RX_RESERVE = re.compile(r"(예매(?!\s*안내)|예약|바로\s*예매|구매|RESERVE|예매하기|Book|Buy)", re.I)
RX_NEXT = re.compile(r"(다음|다음\s*단계|좌석\s*선택(완료)?|좌석선택|Proceed|Next|계속)", re.I)
RX_PAY_TXT = re.compile(r"(결제|결제수단|주문서|PAYMENT|ORDER)", re.I)
RX_ANTI    = re.compile(r"(안내|확인/?취소|취소/확인|유의사항|Guide|Info)", re.I)

INTEREST_KEYS = {
  "prodSeq","sdSeq","perfDate","saleTycd","saleCondNo","planTypeCd","seatTypeCode",
  "chnlCd","csrfToken","sdCode",
  # 추가 수집: 좌석/가격/타입 정보
  "seatId","tkttypSeq","seatClassSeq","ticketPrice","ticketCount"
}
PRICE_PAGE_RX = re.compile(r"가격|티켓선택|일반|청소년|티켓 유형|Price", re.I)

# ----- step decorator (safe & lazy TR) -----
def trace_step(name: str):
    """
    - TR(Tracer)이 아직 준비되지 않아도 안전하게 동작(지연 조회)
    - async/sync 함수 모두 지원
    """
    import functools, asyncio, time

    def deco(fn):
        if asyncio.iscoroutinefunction(fn):
            @functools.wraps(fn)
            async def wrap(*a, **k):
                t0 = time.time()
                tr = globals().get("TR", None)   # ← 여기서 매 호출 시점에 TR을 조회
                try:
                    if tr: tr.ev("step.begin", name=name)
                    r = await fn(*a, **k)
                    if tr: tr.ev("step.end", name=name, ms=int((time.time()-t0)*1000))
                    return r
                except Exception as e:
                    if tr: tr.err("step.error", name=name, err=str(e))
                    raise
            return wrap
        else:
            @functools.wraps(fn)
            def wrap(*a, **k):
                t0 = time.time()
                tr = globals().get("TR", None)
                try:
                    if tr: tr.ev("step.begin", name=name)
                    r = fn(*a, **k)
                    if tr: tr.ev("step.end", name=name, ms=int((time.time()-t0)*1000))
                    return r
                except Exception as e:
                    if tr: tr.err("step.error", name=name, err=str(e))
                    raise
            return wrap
    return deco

# === ADD: resMain → booking 진입 보조 ===
def _host(u: str) -> str:
    try:
        from urllib.parse import urlparse
        return urlparse(u).netloc or ""
    except:
        return ""

async def ensure_booking_iframe(p):
    """
    resMain(biff) 페이지에서도 filmonestop booking iframe이나 팝업을 뜨게 유도하고 찾아서 반환.
    이미 떠 있으면 그대로 반환.
    """
    sc = await find_booking_scope(p)
    if sc:
        return sc

    # 예약/바로예매/Next 류 버튼을 눌러 iframe/popup을 띄우기
    candidates = [
        "//a[contains(., '예매') or contains(., '바로예매') or contains(., 'Book')]",
        "//button[contains(., '예매') or contains(., '바로예매') or contains(., 'Book')]",
        "//a[contains(., 'Next') or contains(., '다음')]",
        "//button[contains(., 'Next') or contains(., '다음')]",
        "a[href*='filmonestop.maketicket.co.kr']",
        "button[href*='filmonestop.maketicket.co.kr']",
        "a[onclick*='filmonestop.maketicket.co.kr']",
        "button[onclick*='filmonestop.maketicket.co.kr']",
        ".btn-reserve, .book-btn, [data-action='reserve']",
    ]
    for _ in range(2):
        for sel in candidates:
            try:
                loc = p.locator(sel)
                if await loc.count():
                    await loc.first.click(timeout=800)
                    # 팝업 생길 수도 있으니 잠깐 대기
                    try:
                        await p.wait_for_event("popup", timeout=1200)
                    except Exception:
                        pass
                    await p.wait_for_timeout(500)
                    sc = await find_booking_scope(p)
                    if sc:
                        return sc
            except Exception:
                pass
    return await find_booking_scope(p)

# === ensure filmonestop cookies =============================================
@trace_step("ensure_onestop_cookies")
async def ensure_onestop_cookies(scope_or_page, prodSeq: str, sdSeq: str):
    """
    같은 브라우저 컨텍스트에서 filmonestop booking URL을 한 번 열어
    그 도메인 쿠키를 채워준다. (RS 401 방지)
    """
    from urllib.parse import urlparse
    # filmonestop 오리진 찾기
    def _onestop_origin_from_context(scope_or_page) -> str:
        page = getattr(scope_or_page, "page", None) or scope_or_page
        ctx  = getattr(page, "context", None)
        if ctx:
            for p in ctx.pages:
                try:
                    u = p.url or ""
                    if "filmonestop.maketicket.co.kr" in u:
                        pu = urlparse(u)
                        return f"{pu.scheme}://{pu.netloc}"
                except:
                    pass
        return "https://filmonestop.maketicket.co.kr"

    page = getattr(scope_or_page, "page", None) or scope_or_page
    ctx  = getattr(page, "context", None) if page else None
    if not ctx:
        return

    origin = _onestop_origin_from_context(scope_or_page)
    # 서비스에 따라 '/ko' 유무만 다를 수 있음. 기본은 /ko 유지.
    url = f"{origin}/ko/onestop/booking?prodSeq={prodSeq}&sdSeq={sdSeq}"

    # 같은 컨텍스트에서 새 탭으로 살짝 열었다 닫기
    p = await ctx.new_page()
    try:
        await p.goto(url, wait_until="domcontentloaded", timeout=15000)
    except Exception:
        pass
    try:
        await p.close()
    except Exception:
        pass

@trace_step("ensure_perf_and_csrf")
async def ensure_perf_and_csrf(scope, prodSeq: str, sdSeq: str) -> tuple[str, str]:
    """perfDate(yyyymmdd)와 csrfToken을 최대한 안전하게 확보한다."""
    # 1) DOM/URL/window에서 먼저 긁기
    try:
        hp = await harvest_params_from_dom(scope)
    except Exception:
        hp = {}
    perfDate = (hp.get("perfDate") or "").strip()
    csrf     = (hp.get("csrfToken") or "").strip()

    # 2) perfDate가 비면 /rs/prod 의 listSch로 보강 (이미 갖춘 유틸 재사용)
    if not perfDate:
        try:
            # _load_all_schedules 는 seq2date( sdSeq -> yyyymmdd )를 리턴함
            _prod, _chn, _ty, _cond, _csrf, _s2c, _c2s, seq2date = await _load_all_schedules(scope)
            perfDate = (seq2date.get(int(sdSeq)) if sdSeq else "") or perfDate
        except Exception:
            pass

    # 3) csrf가 비면 쿠키(XSRF/CSRF)에서 최후 보강
    if not csrf:
        try:
            page = getattr(scope, "page", None) or scope
            ctx  = getattr(page, "context", None)
            if ctx:
                for c in await ctx.cookies():
                    if not isinstance(c, dict):
                        continue
                    if c.get("name") in ("XSRF-TOKEN", "CSRF-TOKEN", "X-CSRF-TOKEN"):
                        csrf = c.get("value") or csrf
                        break
        except Exception:
            pass

    if perfDate and "-" in perfDate:
        perfDate = perfDate.replace("-", "")
    return perfDate, csrf

async def fetch_basic_meta(scope0, prodSeq, sdSeq, *, chnlCd="WEB", perfDate="", csrfToken="", saleCondNo="1"):
    REFS = build_onestop_referers(scope0, str(prodSeq), str(sdSeq))
    H_RS = _ref_headers(REFS, "rs")
    venue = ""
    try:
        ps = await post_api(scope0, "/rs/prodSummary", {
            "langCd":"ko","csrfToken": csrfToken or "",
            "prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
            "chnlCd": chnlCd, "perfDate": perfDate or "",
            "saleCondNo": saleCondNo or "1",
        }, extra_headers=H_RS)
        summ = ps.get("summary") if isinstance(ps, dict) else {}
        if isinstance(summ, list) and summ: summ = summ[0]
        for k in ("operHallNm","operHallName","hallNm","hallName","siteNm","siteName","placeNm","placeName","screenNm","screenName"):
            v = (summ.get(k) if isinstance(summ, dict) else None) or ""
            if v: venue = str(v).strip(); break
    except: 
        pass
    return {"venue": venue}


# === PATCH [AUTO COLLECT] =====================================================
async def _pick_from_dom_or_global(scope):
    js = """
    () => {
      const pick = sel => document.querySelector(sel)?.value || "";
      const qs = new URL(location.href).searchParams;
      const out = {
        prodSeq:  pick("#prodSeq") || (window.ONE_STOP_INFO?.prodSeq ?? qs.get("prodSeq") ?? ""),
        sdSeq:    pick("#sdSeq")   || (window.ONE_STOP_INFO?.sdSeq   ?? qs.get("sdSeq")   ?? ""),
        perfDate: (pick("#perfDate") || (window.ONE_STOP_INFO?.perfDate ?? qs.get("perfDate") ?? "")).replace(/-/g,""),
        csrfToken: pick("#csrfToken") || (window._csrf ?? "")
      };
      return out;
    }
    """
    try:
        return await scope.evaluate(js) or {}
    except Exception:
        return {}

async def _get_cookie(scope, name: str) -> str:
    try:
        return await scope.evaluate(f"""() => (document.cookie.split('; ').find(s=>s.startsWith('{name}='))||'').split('=')[1]||''""")
    except Exception:
        return ""

async def ensure_full_rs_params(scope, prodSeq: str|None, sdSeq: str|None) -> dict:
    """
    prodSeq/sdSeq를 넘기면 나머지(perfDate/csrfToken)를 자동으로 보강.
    DOM/글로벌/URL → 실패 시 /rs/prod(listSch) 역조회 → 쿠키(XSRF) 순.
    """
    # ✅ 프레임 보정
    if not scope:
        raise RuntimeError("ensure_full_rs_params: scope is None")
    try:
        scope = await ensure_filmonestop_scope(scope)
    except Exception:
        pass
    out = {"prodSeq": str(prodSeq or ""), "sdSeq": str(sdSeq or ""), "perfDate": "", "csrfToken": ""}

    # 1) DOM/글로벌/URL
    prim = await _pick_from_dom_or_global(scope)
    out["prodSeq"]   = out["prodSeq"]   or (prim.get("prodSeq") or "")
    out["sdSeq"]     = out["sdSeq"]     or (prim.get("sdSeq") or "")
    out["perfDate"]  = (prim.get("perfDate") or "").replace("-", "") or ""
    out["csrfToken"] = prim.get("csrfToken") or out.get("csrfToken") or ""

    # 2) perfDate가 비면 /rs/prod(listSch)에서 sdSeq→날짜 역조회
    if not out["perfDate"] and out["prodSeq"]:
        try:
            REFS = build_onestop_referers(scope, out["prodSeq"], out["sdSeq"] or "")
            H_RS = _ref_headers(REFS, "rs")
            js = await post_api(scope, "/rs/prod", {
                "prodSeq": out["prodSeq"], "sdSeq": "",
                "chnlCd": "WEB", "saleTycd": "SALE_NORMAL",
                "saleCondNo": "1", "perfDate": "", "csrfToken": out["csrfToken"]
            }, extra_headers=H_RS)
            sch = (js or {}).get("listSch") or []
            sdWanted = out["sdSeq"]
            for it in _iter_dicts(sch):
                sseq = str(it.get("sdSeq") or it.get("sd_seq") or it.get("sdNo") or "")
                if sdWanted and sseq == str(sdWanted):
                    pd = (it.get("sdStartDt") or it.get("sdStartDay") or it.get("perfStartDay") or "")
                    pd = re.sub(r"[^0-9]", "", pd)[:8]
                    if len(pd) == 8: out["perfDate"] = pd
                    break
        except Exception as e:
            dlog(f"[COLLECT] listSch fallback fail: {e}")

    # 3) csrfToken 비면 쿠키(XSRF)로 보강
    if not out["csrfToken"]:
        out["csrfToken"] = await _get_cookie(scope, "XSRF-TOKEN") or await _get_cookie(scope, "CSRF-TOKEN") or ""

    # 4) 마무리 정규화
    out["prodSeq"]  = str(out["prodSeq"] or "")
    out["sdSeq"]    = str(out["sdSeq"] or "")
    out["perfDate"] = re.sub(r"[^0-9]", "", out["perfDate"] or "")[:8]
    dlog(f"[COLLECT] prodSeq={out['prodSeq']} sdSeq={out['sdSeq']} perfDate={out['perfDate']} csrfToken={(out['csrfToken'][:8]+'…') if out['csrfToken'] else ''}")
    return out


# ============================================================================ #

# === CSRF STICKY CACHE =========================================
import time, asyncio
CSRFTOKEN_CACHE = {"val": None, "ts": 0}

MAIN_HOST   = "https://filmonestop.maketicket.co.kr"
API_HOST    = "https://filmonestopapi.maketicket.co.kr"
DEFAULT_REF = f"{MAIN_HOST}/ko/onestop/booking"

async def get_csrf_token_hard(page):
    """프레임/DOM/스토리지/쿠키를 종합적으로 뒤져서 토큰을 강제로 확보."""
    # 1) 프레임에서 hidden input / meta 시도
    for fr in page.frames:
        try:
            v = await fr.evaluate("""() => {
                const byInput = document.querySelector("input[name=csrfToken]")?.value;
                const byMeta  = document.querySelector('meta[name="csrf-token"]')?.content;
                return byInput || byMeta || null;
            }""")
            if v: return v
        except: pass

    # 2) sessionStorage/localStorage 시도
    try:
        v = await page.evaluate("""() => 
            sessionStorage.getItem('csrfToken') ||
            localStorage.getItem('csrfToken') || null
        """)
        if v: return v
    except: pass

    # 3) 쿠키 시도 (메인/서브 도메인 모두)
    try:
        ctx = page.context
        for host in (MAIN_HOST, API_HOST):
            for c in await ctx.cookies(host):
                if not isinstance(c, dict):
                    continue
                n, v = c.get("name"), c.get("value")
                if n in ("X-CSRF-TOKEN", "CSRF-TOKEN", "XSRF-TOKEN", "csrfToken") and v:
                    return v
    except: pass

    return None

async def ensure_csrf(page, current_form_tok: str | None):
    """빈 값이면 캐시/하드탐색 → 끝까지 없으면 예외."""
    # 캐시에 유효값 있으면 사용
    if (not current_form_tok) and CSRFTOKEN_CACHE["val"]:
        return CSRFTOKEN_CACHE["val"]

    # 직접 탐색
    tok = current_form_tok
    if not tok:
        tok = await get_csrf_token_hard(page)

    if not tok:
        raise RuntimeError("CSRF token unavailable (would cause 500). Stop before POST.")

    CSRFTOKEN_CACHE["val"] = tok
    CSRFTOKEN_CACHE["ts"]  = time.time()
    return tok
# ===============================================================


# === NETLOG UTILITIES ==========================================
import os, json, time, pathlib, traceback
NETLOG          = int(os.getenv("LOG_POST_BODY", "1"))          # 1=on, 0=off
NETLOG_RAW      = int(os.getenv("LOG_POST_BODY_RAW", "0"))      # 1=raw body 파일 저장
NETLOG_DIR      = os.getenv("LOG_POST_DIR", "./_netlog")        # 파일 저장 위치
NETLOG_MAX      = int(os.getenv("LOG_POST_MAX", "400"))         # 최대 기록 횟수(과다 로그 방지)
NETLOG_SILENTOK = int(os.getenv("LOG_POST_SILENT_OK", "0"))     # 1이면 200 응답은 요약만

_netlog_seq = 0
SENSITIVE_KEYS = {
    "password", "pass", "pin", "card", "cardNo", "cardno",
    "rrn", "resident", "birth", "mobileAuth", "otp", "auth"
}
# csrfToken은 기본 노출 허용(세션 추적용). 완전 가리고 싶으면 아래 True로.
MASK_CSRF = False

def _redact_form(form: dict):
    out = {}
    for k, v in form.items():
        lk = k.lower()
        if lk in SENSITIVE_KEYS or (lk == "csrftoken" and MASK_CSRF):
            out[k] = "***"
        else:
            out[k] = v
    return out

def _short(s: str, n: int = 240) -> str:
    return s if len(s) <= n else s[:n] + "…"

def _write_file(fname: str, content: str):
    try:
        pathlib.Path(NETLOG_DIR).mkdir(parents=True, exist_ok=True)
        with open(pathlib.Path(NETLOG_DIR) / fname, "w", encoding="utf-8") as f:
            f.write(content)
    except Exception as e:
        print(f"[NET] file write fail: {e}")

def _netlog_req(url: str, headers: dict, form: dict | str, raw_body: str) -> int:
    """요청 직전 호출. 콘솔+파일 기록. 고유 seq 반환."""
    global _netlog_seq
    if not NETLOG:
        return 0
    _netlog_seq += 1
    seq = _netlog_seq
    ts  = time.strftime("%Y%m%d-%H%M%S")

    # 콘솔 요약
    safe_hdr = {
        "Content-Type": headers.get("Content-Type"),
        "Origin":       headers.get("Origin"),
        "Referer":      headers.get("Referer"),
        "X-Requested-With": headers.get("X-Requested-With"),
        "X-CSRF-TOKEN": ("***" if headers.get("X-CSRF-TOKEN") and MASK_CSRF else headers.get("X-CSRF-TOKEN"))
    }
    print(f"[NET] ↗️ POST #{seq} {url}")
    print(f"[NET]     headers: {json.dumps(safe_hdr, ensure_ascii=False)}")
    if isinstance(form, dict):
        print(f"[NET]     form: {json.dumps(_redact_form(form), ensure_ascii=False)}")
    else:
        print(f"[NET]     body: {_short(raw_body)}")

    # 파일(요청 원문 + 호출 스택)
    payload = {
        "url": url,
        "headers": headers,
        "form": form if isinstance(form, dict) else None,
        "raw_body": raw_body if (NETLOG_RAW or not isinstance(form, dict)) else None,
        "stack": "".join(traceback.format_stack(limit=14)),
        "ts": ts,
        "seq": seq,
    }
    _write_file(f"{ts}_{seq:04d}_REQ.json", json.dumps(payload, ensure_ascii=False, indent=2))
    if NETLOG_RAW:
        _write_file(f"{ts}_{seq:04d}_REQ.raw.txt", raw_body)
    return seq

def _netlog_resp(seq: int, url: str, status: int, text: str):
    if not NETLOG:
        return
    ts  = time.strftime("%Y%m%d-%H%M%S")
    short = _short(text, 500)
    if status == 200 and NETLOG_SILENTOK:
        print(f"[NET] ↘️ RESP #{seq} {status} {url}")
    else:
        print(f"[NET] ↘️ RESP #{seq} {status} {url} — {_short(short, 200)}")
    _write_file(f"{ts}_{seq:04d}_RESP.txt", f"{status} {url}\n\n{short}")
# ================================================================

# --- add: request 스코프 정규화 유틸 ---
def _as_page(scope):
    """Frame/ElementHandle가 들어와도 Page로 승격"""
    if scope is None:
        return None
    # playwright Frame이면 page 속성이 있음
    pg = getattr(scope, "page", None)
    # 혹시 구버전에서 page가 callable인 경우 대비
    if callable(pg):
        try:
            pg = pg()
        except Exception:
            pg = None
    return pg or scope  # 이미 Page면 그대로

def _request_ctx(scope):
    """scope(Page/Frame) → APIRequestContext 꺼내기"""
    pg = _as_page(scope)
    if pg is None:
        raise RuntimeError("no page available (scope=None)")
    req = getattr(pg, "request", None)
    if req is None:
        # BrowserContext.request 로도 시도
        ctx = getattr(pg, "context", None)
        req = getattr(ctx, "request", None) if ctx else None
    if req is None:
        raise RuntimeError(f"no page.request available (got {type(pg).__name__})")
    return req

# === ADD: normalize seat list ===
def _normalize_seat_list(js):
    """
    다양한 응답 포맷(js)을 list[dict] 로 정규화.
    - str: JSON 텍스트면 로드, 아니면 빈 리스트
    - dict: 흔한 키(list/data/rows/result/seatList) 중 리스트 찾아 반환
    - list: 요소 중 dict 만 남김
    """
    import json
    if isinstance(js, str):
        s = js.strip()
        if s and s[0] in "[{":
            try:
                js = json.loads(s)
            except:
                return []
        else:
            return []
    if isinstance(js, dict):
        for k in ("list", "data", "rows", "result", "seatList"):
            v = js.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        return []
    if isinstance(js, list):
        out = []
        for x in js:
            if isinstance(x, dict):
                out.append(x)
            elif isinstance(x, str):
                s = x.strip()
                if s and s[0] in "{[":
                    try:
                        y = json.loads(s)
                        if isinstance(y, dict): out.append(y)
                        elif isinstance(y, list):
                            out.extend([z for z in y if isinstance(z, dict)])
                    except: pass
        return out
    return []


@trace_step("hold_at_payment")
async def hold_at_payment(p: Page):
    if not PAY_STAY:
        return
    try:
        msg = "[HOLD] 결제창 유지 중 — 창을 직접 닫거나 Ctrl+C 로 종료하세요"
        if PAY_STAY_TIMEOUT_MS > 0:
            msg += f" (최대 {PAY_STAY_TIMEOUT_MS}ms)"
        dlog(msg)
        if PAY_STAY_TIMEOUT_MS > 0:
            await p.wait_for_event("close", timeout=PAY_STAY_TIMEOUT_MS)
        else:
            await p.wait_for_event("close")  # 사용자가 닫을 때까지 대기
    except Exception:
        pass


# --- NEW: ensure we always have a filmonestop scope before RS/SEAT calls ---
@trace_step("ensure_scope_or_spawn")
async def ensure_scope_or_spawn(scope_or_page, prodSeq: str, sdSeq: str):
    """
    filmonestop booking scope 확보 전략:
      1) 이미 떠 있으면 그대로 사용
      2) resMain이면 예매/Next 버튼 눌러 iframe 유도
      3) 최후: 같은 컨텍스트에서 onestop/booking URL을 잠깐 열어 쿠키/스코프 시드
    """
    # 1) 이미 filmonestop 프레임이 있나?
    try:
        sc = await ensure_filmonestop_scope(scope_or_page)
        if sc:
            return sc
    except:
        pass

    # 2) resMain에서 버튼 눌러 iframe 띄우기
    try:
        page = getattr(scope_or_page, "page", None) or scope_or_page
        if page and "biff.maketicket.co.kr" in (page.url or ""):
            sc = await ensure_booking_iframe(page)
            if sc:
                return sc
    except:
        pass

    # 3) 최후 수단: 같은 컨텍스트에서 onestop/booking 직접 열었다 닫고 scope 획득
    try:
        page = getattr(scope_or_page, "page", None) or scope_or_page
        ctx  = getattr(page, "context", None)
        if ctx and prodSeq and sdSeq:
            tmp = await ctx.new_page()
            try:
                keep = False
                await tmp.goto(
                    f"{MAIN_HOST}/ko/onestop/booking?prodSeq={prodSeq}&sdSeq={sdSeq}",
                    wait_until="domcontentloaded", timeout=15000
                )
                sc = await ensure_filmonestop_scope(tmp)
                if sc:
                    # 프레임은 페이지 생명주기에 종속되므로 Page 자체를 반환하고 탭은 유지
                    keep = True
                    return tmp
            finally:
                if not keep:
                    try: await tmp.close()
                    except: pass
    except:
        pass
    return None

    
# === helpers: onestop (filmonestop) origin & referer =========================
def _onestop_origin_from_context(scope_or_page) -> str:
    from urllib.parse import urlparse
    page = getattr(scope_or_page, "page", None) or scope_or_page
    ctx  = getattr(page, "context", None)
    if ctx:
        for p in ctx.pages:
            try:
                u = p.url or ""
                if "filmonestop.maketicket.co.kr" in u:
                    pu = urlparse(u)
                    return f"{pu.scheme}://{pu.netloc}"
            except:
                pass
    return "https://filmonestop.maketicket.co.kr"

# REPLACE: build_onestop_referers (compat)
def build_onestop_referers(scope_or_page, prodSeq: str, sdSeq: str | None = None) -> dict:
    """
    - 2개 인자(기존)와 3개 인자(신규) 모두 허용
    - 항상 rs/seat 각각의 Referer를 반환
    """
    prodSeq = str(prodSeq)
    sdSeq   = "" if sdSeq is None else str(sdSeq)

    base = f"{MAIN_HOST}/ko"   # MAIN_HOST == "https://filmonestop.maketicket.co.kr"
    rs   = f"{base}/onestop/rs?prodSeq={urllib.parse.quote(prodSeq)}&sdSeq={urllib.parse.quote(sdSeq)}"
    seat = f"{base}/onestop/rs/seat?prodSeq={urllib.parse.quote(prodSeq)}&sdSeq={urllib.parse.quote(sdSeq)}"
    return {"rs": rs, "seat": seat}


# === REPLACE: find_booking_scope — prefer payment/price/seat/zone ============
async def find_booking_scope(p):
    """현재 페이지나 iframe들 중 filmonestop booking 영역을 찾아서 반환.
       payment > price > seat/zone > booking 순으로 우선."""
    try:
        frames = [f for f in p.frames if "filmonestop.maketicket.co.kr" in (f.url or "")]
        def score(u: str) -> int:
            u = u or ""
            if "/payment" in u or "/order" in u: return 4
            if "/price"   in u: return 3
            if "/seat"    in u or "/zone" in u: return 2
            if "/booking" in u: return 1
            return 0
        if frames:
            return max(frames, key=lambda f: score(f.url or ""))
        if "filmonestop.maketicket.co.kr" in (p.url or ""):
            return p
    except Exception:
        pass
    return None

# utils/http_headers.py (아무 파일이나 공용 utils로)
from urllib.parse import urlparse

def _ref_headers(refs, key: str = "rs"):
    """
    refs: dict({'rs': <url>, 'seat': <url>}) 또는 그냥 문자열(=rs url)
    반환: {'Referer': <url>, 'Origin': '<scheme>://<host>'}
    """
    # 1) 참조 URL 결정
    if isinstance(refs, dict):
        ref = refs.get(key) or next(iter(refs.values()), "")
    else:
        ref = str(refs or "")

    # 2) 빈 값이면 안전하게 빈 헤더로
    if not ref:
        return {"Referer": "", "Origin": ""}

    # 3) scheme/host 추출
    u = urlparse(ref if ref.endswith("/") else ref + "/")
    origin = f"{u.scheme}://{u.netloc}" if u.scheme and u.netloc else ""

    # 4) Referer는 반드시 슬래시로 끝내기(브라우저 트레이스와 동일)
    referer = ref if ref.endswith("/") else ref + "/"
    return {"Referer": referer, "Origin": origin}


async def is_price_page(scope) -> bool:
    if not scope:
        return False
    try:
        # 가격표/티켓수량 위젯 존재 확인
        if await scope.locator("select[name='rsVolume'], #rsVolume").count():
            return True
        if await scope.locator("select[name='selectedTicketCount'], #selectedTicketCount").count():
            return True
        if await scope.get_by_text(PRICE_PAGE_RX).count():
            return True
        if await scope.get_by_text(re.compile(r"(가격|요금|티켓수량|매수|일반|성인|청소년|티켓 유형|Price)", re.I)).count():
            return True
    except Exception:
        pass
    return False
async def click_any_zone(scope) -> bool:
    """존 선택 페이지에서 첫 가용 존을 하나 눌러준다."""
    try:
        loc = scope.locator(".zone.available, .zone:not(.soldout), [data-zone]:not(.soldout)")
        if await loc.count():
            await loc.first.click(timeout=CLICK_TIMEOUT)
            await scope.wait_for_timeout(400)
            return True
    except:
        pass
    return False

async def _prepare_session_like_har(scope, *, prodSeq, sdSeq, perfDate, csrfToken=None,
                                    chnlCd="WEB", saleTycd="SALE_NORMAL", saleCondNo="1"):
    # 부족하면 자체 보강
    from re import sub as _re_sub
    async def _get_cookie(s, name):
        try:
            return await s.evaluate("n=>{const x=document.cookie.split('; ').find(s=>s.startsWith(n+'='));return x?x.split('=')[1]:''}", name)
        except: return ""
    if not csrfToken:
        csrfToken = await _get_cookie(scope, "XSRF-TOKEN") or await _get_cookie(scope, "CSRF-TOKEN") or ""

    perfDate = _re_sub(r"[^0-9]", "", perfDate or "")[:8]  # yyyymmdd 강제
    REFS = build_onestop_referers(scope, str(prodSeq), str(sdSeq or ''))
    H_RS = _ref_headers(REFS, 'rs')   # ← 안전 접근 (문자열/딕셔너리 모두 OK)

    def P(path, form):
        base = {"langCd": "ko", "csrfToken": csrfToken}
        base.update(form)
        return post_api(scope, f"/rs/{path}", base, extra_headers=H_RS)

    # ✅ prod → prodChk → chkProdSdSeq → informLimit → prodSummary → blockSummary2
    try: await P("prod", {"prodSeq": str(prodSeq)})
    except Exception as e: dlog(f"[SWAP] prod warn: {e}")
    try: await P("prodChk", {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
                             "chnlCd": chnlCd, "saleTycd": saleTycd, "saleCondNo": saleCondNo,
                             "perfDate": perfDate})
    except Exception as e: dlog(f"[SWAP] prodChk warn: {e}")
    try: await P("chkProdSdSeq", {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq), "chnlCd": chnlCd})
    except Exception as e: dlog(f"[SWAP] chkProdSdSeq warn: {e}")
    try: await P("informLimit", {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
                                 "chnlCd": chnlCd, "saleTycd": saleTycd, "saleCondNo": saleCondNo})
    except Exception as e: dlog(f"[SWAP] informLimit warn: {e}")
    try: await P("prodSummary", {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
                                 "chnlCd": chnlCd, "perfDate": perfDate})
    except Exception as e: dlog(f"[SWAP] prodSummary warn: {e}")
    try: await P("blockSummary2", {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq), "chnlCd": chnlCd, "perfDate": perfDate})
    except Exception as e: dlog(f"[SWAP] blockSummary2 warn: {e}")

    ilog("[SWAP] session prepared (prod→prodChk→chkProdSdSeq→informLimit→prodSummary→blockSummary2)")

# === REPLACE ENTIRE FUNCTION: robust NRS counter via /api/v1/rs/tickettype ===
@trace_step("seat_counts_via_tickettype")
async def seat_counts_via_tickettype(scope0, prodSeq, sdSeq, chnlCd="WEB", csrfToken=""):
    """
    자유석(NRS) 총좌석/잔여석을 tickettype에서 안정적으로 가져온다.
    - 필수 폼 필드( HAR 기준 ): perfDate, sdCode, saleTycd, saleCondNo, langCd, jType, rsrvStep 등
    - Referer/Origin: filmonestop 루트 고정
    - 필요 시 세션 준비 후 1회 재시도
    """
    prodSeq = str(prodSeq); sdSeq = str(sdSeq)
    # 🔒 scope 보장
    scope0 = await ensure_scope_or_spawn(scope0, prodSeq, sdSeq) or scope0
    if not scope0:
        raise RuntimeError("filmonestop scope not found (cannot proceed)")

    # 0) 공통 파라미터 확보
    pack = await ensure_full_rs_params(scope0, prodSeq, sdSeq)
    perfDate  = pack.get("perfDate") or ""
    csrfToken = csrfToken or pack.get("csrfToken") or ""

    _prod, chnl, saleTycd, saleCondNo, csrf0, seq2code, code2seq, seq2date = await _load_all_schedules(scope0)
    sdCode = (seq2code.get(int(sdSeq)) if sdSeq.isdigit() else None) or sdSeq.zfill(3)

    # 1) 헤더: filmonestop 오리진으로
    origin = _onestop_origin_from_context(scope0)
    H = {"Referer": f"{origin}/", "Origin": origin}

    # 2) HAR와 동일 스키마로 폼 구성
    def _form():
        return {
            "prodSeq": prodSeq,
            "chnlCd": chnl or chnlCd or "WEB",
            "perfDate": re.sub(r"[^0-9]", "", perfDate or seq2date.get(int(sdSeq), ""))[:8],
            "sdSeq": sdSeq,
            "sdCode": sdCode,
            "saleTycd": saleTycd or "SALE_NORMAL",
            "sdSeqOld": sdSeq,
            "saleTycdOld": saleTycd or "SALE_NORMAL",
            "saleCondNo": saleCondNo or "1",
            "prodTyCd": "NORMAL",
            "planTypeCd": "NRS",
            "seatTypeCode": "NRS",
            "jType": "I",
            "rsrvStep": "TKT",
            "langCd": "ko",
            "csrfToken": csrfToken or csrf0 or ""
        }

    async def _call():
        return await post_api(scope0, "/api/v1/rs/tickettype", _form(), extra_headers=H)

    # 3) 1차 호출 → 실패 시 세션 준비 후 1회 재시도
    try:
        js = await _call()
    except Exception as e1:
        try:
            await _prepare_session_like_har(scope0, prodSeq=prodSeq, sdSeq=sdSeq,
                                            perfDate=perfDate or seq2date.get(int(sdSeq), ""), csrfToken=csrfToken or csrf0)
        except Exception as eprep:
            dlog(f"[SEAT] tickettype prepare warn: {eprep}")
        js = await _call()  # 재시도 (여기서도 예외면 상위에서 잡힘)

    # 4) 집계
    seat_list = (js.get("seatList") if isinstance(js, dict) else []) or []
    total = remain = 0
    by = {}

    # seatNo=="" 집계 레코드를 우선 사용
    aggs = [s for s in seat_list if (s.get("seatTypeCd") == "NRS" and (s.get("seatNo") or "") == "")]
    if aggs:
        s = aggs[0]
        avail = int(s.get("admissionAvailPersonCnt") or s.get("restSeatCnt") or 0)  # 총
        sold  = int(s.get("admissionPersonCnt") or 0)                               # 판매
        total  = max(total, avail)
        remain = max(remain, max(avail - sold, 0))
    else:
        # 없으면 NRS 리스트 최댓값/차이로 보정
        for s in seat_list:
            if s.get("seatTypeCd") != "NRS":
                continue
            avail = int(s.get("admissionAvailPersonCnt") or s.get("saleSeatCnt") or s.get("rendrSeatCnt") or 0)
            sold  = int(s.get("admissionPersonCnt") or 0)
            total  = max(total, avail)
            remain = max(remain, max(avail - sold, 0))

    if total or remain:
        by["NRS"] = remain

    return total, remain, by, ("NRS" if total > 0 else "ALL")




# REPLACE: fetch_seat_summary (핵심만 발췌)
@trace_step("fetch_seat_summary")
async def fetch_seat_summary(scope0, prodSeq, sdSeq,
                             chnlCd="WEB", saleTycd="SALE_NORMAL",
                             csrfToken="", saleCondNo="1", perfDate=""):
    # 🔒 scope 보장
    scope0 = await ensure_scope_or_spawn(scope0, str(prodSeq), str(sdSeq)) or scope0
    if not scope0:
        raise RuntimeError("filmonestop scope not found (cannot proceed)")
    await ensure_onestop_cookies(scope0, str(prodSeq), str(sdSeq))
    REFS   = build_onestop_referers(scope0, str(prodSeq), str(sdSeq))
    H_RS   = _ref_headers(REFS, "rs")
    H_SEAT = _ref_headers(REFS, "seat")

    pd, ct = await ensure_perf_and_csrf(scope0, str(prodSeq), str(sdSeq))
    perfDate  = perfDate  or pd or ""
    csrfToken = csrfToken or ct or ""

    # 0) plan_type 선조회
    plan_type = ""
    try:
        base = await post_api(scope0, "/seat/GetRsSeatBaseMap",
            {"prod_seq": str(prodSeq), "sd_seq": str(sdSeq),
             "chnl_cd": chnlCd, "sale_tycd": saleTycd},
            extra_headers=H_SEAT)
        b = base if isinstance(base, dict) else {}
        plan_type = b.get("plan_type") or b.get("planType") or ""
    except: pass

    # 0-1) 자유석이면 즉시 blockSummary2로 총/잔여 확정
    if plan_type in ("NRS","FREE","RS"):
        try:
            blk = await post_api(scope0, "/rs/blockSummary2",
                {"langCd":"ko","csrfToken":csrfToken,"prodSeq":str(prodSeq),
                 "sdSeq":str(sdSeq),"chnlCd":chnlCd,"perfDate":perfDate},
                extra_headers=H_RS)
            summ = blk.get("summary") if isinstance(blk, dict) else {}
            if isinstance(summ, list) and summ: summ = summ[0]
            avail = int(summ.get("admissionAvailPersonCnt") or summ.get("restSeatCnt") or 0)
            total = int(summ.get("admissionTotalPersonCnt") or summ.get("saleSeatCnt") or summ.get("rendrSeatCnt") or 0)
            if total <= 0: total = max(total, avail)  # 매진 케이스 보정
            return total, avail, {"NRS": avail}, "NRS"
        except Exception as e:
            dlog(f"[SEAT] NRS-first failed: {e}")

    # 1) 지정석: seatStatusList → zone 합산 폴백
    lst = await _fetch_seat_status_list(scope0, prodSeq, sdSeq, chnlCd=chnlCd, csrfToken=csrfToken, extra_headers=H_SEAT)
    lst = _normalize_seat_list(lst)
    total, remain, by = _count_seats(lst, AVAILABLE_CODES)

    if total == 0 and remain == 0:
        # 👉 tickettype은 자유석(NRS) 플로우에서만 사용 (지정석은 500 유발 가능)
        if (plan_type or "").upper() in ("NRS", "FREE", "RS") or not plan_type:
            try:
                tN, rN, byN, planN = await seat_counts_via_tickettype(
                    scope0, prodSeq, sdSeq, chnlCd=chnlCd, csrfToken=csrfToken
                )
                if tN or rN:
                    return tN, rN, (byN or {}), (planN or plan_type or "ALL")
            except Exception as e:
                dlog(f"[SEAT] tickettype fallback failed: {e}")

        # (기존) ZONE 폴백 유지
        t2, r2, by2, plan2 = await seat_summary_zone_only(scope0, prodSeq, sdSeq, chnlCd=chnlCd, saleTycd=saleTycd)
        if t2 or r2:
            return t2, r2, by2, (plan2 or plan_type or "ALL")

    return total, remain, by, (plan_type or "ALL")


# --- ADD: 필수 키/파라미터 보강 & 누락체크 ---

from urllib.parse import urlparse, parse_qs

REQ_KEYS = ("prodSeq", "sdSeq", "perfDate", "csrfToken")
def _missing(params: dict) -> list[str]:
    return [k for k in REQ_KEYS if not params.get(k)]

def _first(qs: dict, k: str) -> str | None:
    v = qs.get(k)
    return (v[0] if isinstance(v, list) and v else None)

async def harvest_params_from_dom(scope) -> dict:
    """
    URL 쿼리스트링 + hidden input + window 전역 객체에서 파라미터를 최대한 채집.
    """
    out = {}
    try:
        # URL
        u = scope.url or ""
        qs = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(u).query or ""))
        for k in ["prodSeq", "sdSeq", "perfDate", "sdCode", "seatId"]:
            if qs.get(k):
                out[k] = qs[k]

        # hidden inputs
        for name in ["csrfToken", "csrf", "prodSeq", "sdSeq", "perfDate", "sdCode"]:
            try:
                loc = scope.locator(f"input[name='{name}'], #{name}")
                if await loc.count():
                    val = await loc.first.input_value()
                    if val:
                        out[name] = val
            except Exception:
                pass

        # window 전역 (사이트에 따라 다름)
        try:
            w = await scope.evaluate("({ONE_STOP_INFO, _csrf}) => ({ONE_STOP_INFO, _csrf})")
            if isinstance(w, dict):
                if isinstance(w, dict) and w.get("_csrf") and not out.get("csrfToken"):
                    out["csrfToken"] = w.get("_csrf")
                    out["csrfToken"] = w["_csrf"]
                info = w.get("ONE_STOP_INFO") or {}
                for k in ["prodSeq", "sdSeq", "perfDate", "sdCode"]:
                    if info.get(k) and not out.get(k):
                        out[k] = str(info[k])
        except Exception:
            pass

        # perfDate 형식 보정 (yyyy-mm-dd → yyyymmdd)
        if out.get("perfDate") and "-" in out["perfDate"]:
            out["perfDate"] = out["perfDate"].replace("-", "")
    except Exception:
        pass
    return out

@dataclass
class RunResult:
    sd: str
    title: str
    ok: bool
    url: str
    reason: str = ""
    # 👇 추가
    total: int = -1
    remain: int = -1
    plan: str = ""
    venue: str = ""       # 극장/홀명
    perfDate: str = ""    # yyyymmdd

def log(*a): print(*a)

# ----- Structured tracer -----
class Tracer:
    def __init__(self):
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.f = (TRACE_DIR / f"trace_{ts}.jsonl").open("a", encoding="utf-8")
        self.sid = str(uuid.uuid4())[:8]
    def _emit(self, level, event, **fields):
        if not TRACE_ENABLE: 
            return
        rec = {"ts": time.time(), "lvl": level, "ev": event, **fields}
        try:
            self.f.write(json.dumps(rec, ensure_ascii=False) + "\n"); self.f.flush()
        except Exception:
            pass
    def ev(self, event, **kw):   self._emit("INFO", event, **kw)
    def warn(self, event, **kw): self._emit("WARN", event, **kw)
    def err(self, event, **kw):  self._emit("ERR",  event, **kw)
    async def dump_html(self, page, label):
        try:
            p = TRACE_DIR / f"{label}.html"
            await page.wait_for_timeout(50)
            html = await page.content()
            p.write_text(html, encoding="utf-8")
            self.ev("dump.html", path=str(p))
        except Exception as e:
            self.warn("dump.html.fail", reason=str(e))
    async def dump_png(self, page, label):
        try:
            p = TRACE_DIR / f"{label}.png"
            await page.screenshot(path=str(p), full_page=False)
            self.ev("dump.png", path=str(p))
        except Exception as e:
            self.warn("dump.png.fail", reason=str(e))

TR = Tracer()

# ----- Reason collector (per sd) -----
class Reasons:
    def __init__(self, sd):
        self.sd = sd
        self.items = []
    def add(self, code, **ctx):
        self.items.append({"code": code, **ctx})
        tr = globals().get("TR", None)
        if tr: tr.ev("reason", sd=self.sd, code=code, **ctx)
        

# ---------------- 공통 ----------------
async def looks_like_login(p: Page) -> bool:
    try:
        if p.is_closed(): return True
        if "/login" in (p.url or ""): return True
        if await p.locator("input[type='password']").count(): return True
        has_login  = await p.locator("a:has-text('로그인'), button:has-text('로그인')").count()
        has_logout = await p.locator("a:has-text('로그아웃'), button:has-text('로그아웃')").count()
        return has_login and not has_logout
    except: return False

async def wait_logged_in(p: Page, timeout_ms=150_000) -> bool:
    try:
        await p.wait_for_function("""
          () => !location.pathname.includes('login') &&
                !document.querySelector("input[type='password']")
        """, timeout=timeout_ms)
        return True
    except: return False

async def find_title(p: Page) -> str:
    if p.is_closed(): return ""
    sels = ["meta[property='og:title']","h1","h2",".title",".tit",".movie-title",".program-title",".prod-title",".page-title"]
    for s in sels:
        try:
            if s.startswith("meta"):
                el = p.locator(s)
                if await el.count():
                    v = (await el.first.get_attribute("content") or "").strip()
                    if v: return re.sub(r"\s+"," ", v)
            else:
                el = p.locator(s)
                if await el.count():
                    t = (await el.first.inner_text()).strip()
                    if t: return re.sub(r"\s+"," ", t)
        except: pass
    try: return re.sub(r"\s+"," ", (await p.title()) or "")
    except: return ""

def _merge(dst: Dict[str,Any], src: Dict[str,Any]):
    for k in INTEREST_KEYS:
        v = src.get(k)
        if v not in (None,""): dst[k]=v

import random

@trace_step("pick_seat_via_api")
async def pick_seat_via_api(p, prodSeq: str, sdSeq: str) -> tuple[str|None, str|None]:
    """
    GetRsSeatStatusList 응답에서 '구매 가능' 좌석 1개 랜덤 추출.
    SS01000 우선, 없으면 SS02000/SS03000 포함해서 재시도.
    """
    try:
        js = await fetch_json(
            p,
            "https://filmonestopapi.maketicket.co.kr/api/v1/rs/seat/GetRsSeatStatusList",
            {"prod_seq": prodSeq, "sd_seq": sdSeq, "chnl_cd": "WEB", "timeStemp": ""}
        )
        js = _normalize_seat_list(js)
        if not isinstance(js, list):
            return (None, None)

        def pool(codes: set[str]):
            return [
                (str(d.get("seat_id")), str(d.get("seat_class_seq") or "1"))
                for d in js
                if (d.get("use_yn") == "Y") and (d.get("seat_status_cd") in codes) and d.get("seat_id")
            ]

        # 1순위: SS01000
        candidates = pool({"SS01000"})
        if not candidates:
            # 2순위: SS01000 + SS02000 + SS03000
            candidates = pool({"SS01000", "SS02000", "SS03000"})

        if not candidates:
            return (None, None)

        random.shuffle(candidates)
        return candidates[0]
    except Exception:
        return (None, None)


@trace_step("wait_params_from_network")
async def wait_params_from_network(p: Page, timeout_ms=10_000) -> Dict[str,str]:
    """booking 페이지의 API 요청에서 prodSeq/sdSeq/perfDate/csrfToken/sdCode 등을 수집"""
    bucket: Dict[str,Any] = {}
    done = asyncio.get_event_loop().create_future()

    async def on_req(req):
        try:
            url = req.url
            if "maketicket.co.kr/api" not in url: return
            q = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query or ""))
            _merge(bucket, q)
            if req.post_data:
                post = dict(urllib.parse.parse_qsl(req.post_data))
                _merge(bucket, post)
            if all(k in bucket for k in ["prodSeq","sdSeq","perfDate","csrfToken"]):
                if not done.done(): done.set_result(True)
        except: pass

    p.on("request", lambda r: asyncio.create_task(on_req(r)))
    try:
        await asyncio.wait_for(done, timeout=timeout_ms/1000.0)
    except asyncio.TimeoutError:
        pass

    # DOM 보강
    if "csrfToken" not in bucket:
        try:
            token = await p.eval_on_selector("[name='csrfToken'], #csrfToken", "el=>el&&el.value")
            if token: bucket["csrfToken"]=token
        except: pass

    return {k:str(v) for k,v in bucket.items() if k in INTEREST_KEYS}

@trace_step("seat_summary_zone_only")
async def seat_summary_zone_only(scope0, prodSeq, sdSeq, chnlCd="WEB", saleTycd="SALE_NORMAL"):
    REFS = build_onestop_referers(scope0, str(prodSeq), str(sdSeq))
        # headers for RS endpoints
    ORI  = _onestop_origin_from_context(scope0)
    H_RS = {'Referer': REFS.get('rs', REFS.get('prod','')), 'Origin': ORI}
    refs = build_onestop_referers(scope0, prodSeq, sdSeq)
    H_SEAT = _ref_headers(refs, 'seat')

    base = await post_api(
        scope0, "/seat/GetRsSeatBaseMap",
        {"prod_seq": str(prodSeq), "sd_seq": str(sdSeq), "chnl_cd": chnlCd, "sale_tycd": saleTycd},
        extra_headers=H_SEAT
    )
    b = base if isinstance(base, dict) else {}
    plan_type = b.get("plan_type") or b.get("planType") or ""
    # 존/좌석 설계가 아닌 경우는 폴백 불가
    if plan_type != "ZONE":
        return 0, 0, {}, (plan_type or "UNKNOWN")

    # zone id 추출(키 변형 허용)
    zone_ids = []
    if isinstance(base, dict):
        for key in ["zoneList","zones","list","items","data","zd"]:
            v = base.get(key)
            if isinstance(v, list):
                for z in _iter_dicts(v):
                    zid = z.get("zone_id") or z.get("zoneId") or z.get("id") or z.get("zid")
                    if zid: zone_ids.append(zid)

    total = remain = 0
    by = {}
    for zid in zone_ids:
        zi = await post_api(
            scope0, "/seat/GetRsZoneSeatMapInfo",
            {"prod_seq": str(prodSeq), "sd_seq": str(sdSeq), "chnl_cd": chnlCd,
             "zone_id": zid, "sale_tycd": saleTycd, "timeStemp": ""},
            extra_headers=H_SEAT
        )
        zid = zi if isinstance(zi, dict) else {}
        seats = zid.get("seats") or zid.get("seatList") or []
        for s in _iter_dicts(seats):
            if (s.get("use_yn") or s.get("useYn")) != "Y": 
                continue
            st = (s.get("seat_status_cd") or s.get("seatStatusCd") or "")
            by[st] = by.get(st, 0) + 1
            total += 1
            if st in {"SS01000","SS02000","SS03000"}:
                remain += 1
    return total, remain, by, "ZONE"

@trace_step("force_pick_one_seat")
async def force_pick_one_seat(scope, prodSeq: str, sdSeq: str) -> bool:
    """DOM 좌석 클릭 실패 시 seat_id 기반으로 강제 선택 시도."""
    # 1) API로 좌석 하나 고르기
    seatId, seatClassSeq = await pick_seat_via_api(scope, prodSeq, sdSeq)
    if not seatId:
        # DOM에서 혹시 보이는 'available' 한 칸 찍기
        return await pick_any_seat(scope)

    # 2) seatId로 해당 엘리먼트 찾아 강제 클릭(여러 셀렉터 시도)
    sels = [
        f"[data-seat-id='{seatId}']",
        f"[data-id='{seatId}']",
        f"[id='{seatId}']",
        f".seat[id*='{seatId}']",
        f"g.seat[id*='{seatId}'], rect.seat[id*='{seatId}']"
    ]
    for sel in sels:
        try:
            loc = scope.locator(sel)
            if await loc.count():
                await loc.first.click(timeout=800)
                await scope.wait_for_timeout(120)
                return True
        except:
            pass
    # 마지막 시도: 좌석 전수 중 첫 available
    return await pick_any_seat(scope)


# --- helper: iterate only dict items (defensive against mixed API payloads) ---
def _iter_dicts(x):
    if isinstance(x, dict):
        # sometimes wrapped like {"list":[...]}
        for k in ("list","items","data","rows","result","seatList","zones","zoneList"):
            v = x.get(k)
            if isinstance(v, list):
                for it in v:
                    if isinstance(it, dict):
                        yield it
        return
    for it in (x or []):
        if isinstance(it, dict):
            yield it

# === helpers: origin & referer ===============================================
def _biff_origin_from_context(scope_or_page) -> str:
    from urllib.parse import urlparse
    page = getattr(scope_or_page, "page", None) or scope_or_page
    ctx  = getattr(page, "context", None)
    if ctx:
        for p in ctx.pages:
            try:
                u = p.url or ""
                if "biff.maketicket.co.kr" in u:
                    pu = urlparse(u)
                    return f"{pu.scheme}://{pu.netloc}"
            except:
                pass
    return "https://biff.maketicket.co.kr"

def build_onestop_referer(scope_or_page, prodSeq: str, sdSeq: str) -> str:
    origin = _biff_origin_from_context(scope_or_page)
    # 실제 서비스 경로는 /ko/onestop/booking?prodSeq=...&sdSeq=... (HAR 계열도 동일)
    return f"{origin}/ko/onestop/booking?prodSeq={prodSeq}&sdSeq={sdSeq}"

# ---- tickettype 파싱 유틸: tkttypSeq, seatClassSeq, ticketPrice 뽑기 ----
def _pick_ticket_option(js: Any) -> tuple[str,str,str]:
    tkttypSeq, seatClassSeq, ticketPrice = "1","1","10000"
    def _as_list(obj):
        if isinstance(obj, dict):
            for k in ["list","items","ticketTypes","data","rows"]:
                v = obj.get(k)
                if isinstance(v, list) and v:
                    return [x for x in v if isinstance(x, dict)]
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
        return []
    def _price(d):
        for k in ["ticketPrice","price","saleAmt","amt","amount"]:
            v = d.get(k)
            if v is not None:
                try: return str(int(float(str(v).replace(",",""))))
                except: return str(v)
        return ticketPrice
    for it in _iter_dicts(_as_list(js)):
        tkttypSeq  = str(it.get("tkttypSeq") or it.get("id") or tkttypSeq)
        seatClassSeq = str(it.get("seatClassSeq") or it.get("seatClass") or seatClassSeq)
        ticketPrice  = _price(it)
        break
    return tkttypSeq, seatClassSeq, ticketPrice

def _yyyymmdd_to_dash(d: str) -> str:
    return d if "-" in (d or "") else f"{d[:4]}-{d[4:6]}-{d[6:]}"


# --- 항상 #oneStopFrame (filmonestop) 프레임을 최우선으로 고르는 헬퍼 ---
async def _prefer_onestop_frame(page_or_frame):
    """Page/Frame 무엇이 들어와도 #oneStopFrame → filmonestop 프레임 → 현재 스코프로 정규화"""
    # 1) Page면 #oneStopFrame 먼저
    try:
        page = getattr(page_or_frame, "page", None) or (page_or_frame if hasattr(page_or_frame, "frames") else None)
        if page:
            try:
                el = page.locator("#oneStopFrame")
                if await el.count():
                    fr = await el.first.content_frame()
                    if fr and "filmonestop.maketicket.co.kr" in (fr.url or ""):
                        return fr
            except:  # 없으면 아래로
                pass
            # 2) filmonestop 프레임 직접 스캔
            for fr in page.frames:
                if "filmonestop.maketicket.co.kr" in (fr.url or ""):
                    return fr
            # 3) 마지막: 그대로 반환
            return page_or_frame
    except:
        pass
    return page_or_frame


# === origin 선택: filmonestop 스코프 강제 확보 ===============================
FILM_ONESTOP_HOST = "filmonestop.maketicket.co.kr"

@trace_step("ensure_filmonestop_scope")
async def ensure_filmonestop_scope(page_or_pop, timeout_ms=12000):
    """
    filmonestop 오리진에서 실행 가능한 Frame/Page을 반환.
    1) #oneStopFrame.contentFrame() 최우선
    2) 그 다음 filmonestop URL 가진 프레임
    3) 반복 스캔
    """
    import time
    t0 = time.time()

    # 0) 즉시 후보 (#oneStopFrame 우선)
    cand = await _prefer_onestop_frame(page_or_pop)
    try:
        origin = await cand.evaluate("location.origin")
        if FILM_ONESTOP_HOST in origin:
            return cand
    except:  # cand가 Page(다른 오리진)일 수 있으니 계속 탐색
        pass

    # 1) 루프 스캔
    while (time.time() - t0) * 1000 < timeout_ms:
        page = getattr(page_or_pop, "page", None) or page_or_pop
        pages = []
        try:
            ctx = getattr(page, "context", None)
            pages = ctx.pages if ctx else []
        except:
            pages = []

        for p in pages:
            # #oneStopFrame 최우선
            try:
                el = p.locator("#oneStopFrame")
                if await el.count():
                    fr = await el.first.content_frame()
                    if fr:
                        try:
                            ori = await fr.evaluate("location.origin")
                            if FILM_ONESTOP_HOST in (ori or ""):
                                return fr
                        except: pass
            except: pass

            # 일반 프레임 스캔
            for fr in p.frames:
                try:
                    ori = await fr.evaluate("location.origin")
                except:
                    ori = ""
                if FILM_ONESTOP_HOST in ori or "filmonestop.maketicket.co.kr" in (fr.url or ""):
                    return fr
        await page_or_pop.wait_for_timeout(250)

    raise RuntimeError("filmonestop scope not found (oneStopFrame/frames not ready)")

@trace_step("api_bases")
async def api_bases(scope_or_page):
    """
    - ston: 좌석/RS 모두 쓰는 'filmonestopapi' 오리진
    - prox: BIFF 프록시 (백업)
    """
    # ston (window 변수 있으면 사용, 없으면 기본값)
    try:
        ston = await (await ensure_filmonestop_scope(scope_or_page)).evaluate(
            "() => (window.stonOapi || 'https://filmonestopapi.maketicket.co.kr').replace(/\\/$/, '')"
        )
    except Exception:
        ston = "https://filmonestopapi.maketicket.co.kr"

    # prox (biff 오리진)
    prox = "https://biff.maketicket.co.kr/proxy/onestop"
    return {"ston": ston, "prox": prox}

# === REPLACE ENTIRE FUNCTION: post_api ===
async def post_api(scope_or_page, path: str, form: dict | str,
                   timeout_ms: int = 15000, extra_headers: dict | None = None):
    # rs 계열 & prod 제외는 무조건 토큰 필요
    needs_csrf = (
        path.startswith("/api/v1/rs/") or path.startswith("/rs/") or
        path.startswith("/api/v1/seat/") or path.startswith("/seat/")
    ) and not path.endswith("/prod")
    if isinstance(form, dict) and needs_csrf:
        if not form.get("csrfToken"):
            form = dict(form)
            page = getattr(scope_or_page, "page", None) or scope_or_page
            form["csrfToken"] = await ensure_csrf(page, form.get("csrfToken"))

    # 항상 메인 호스트로 Origin/Referer 기본
    base_headers = {"Origin": MAIN_HOST, "Referer": DEFAULT_REF}
    if needs_csrf and CSRFTOKEN_CACHE.get("val"):
        base_headers.setdefault("X-CSRF-TOKEN", CSRFTOKEN_CACHE["val"])

    # x-www-form-urlencoded 강제 (maketicket 기본)
    from urllib.parse import urlencode
    if isinstance(form, dict):
        payload = urlencode(form or {})
        base_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    else:
        payload = form

    if extra_headers:
        base_headers.update(extra_headers)

    url = f"{API_HOST}{path}"
    return await fetch_json(
        scope_or_page,
        method="POST",
        url=url,
        data=payload,
        headers=base_headers,
        timeout=float(timeout_ms),
    )




# === REPLACE ENTIRE FUNCTION: fetch_json ===
@trace_step("fetch_json")
async def fetch_json(scope_or_page, *args, **kwargs):
    """
    Backward compatible:
      - NEW: fetch_json(p, method="POST", url="...", data=..., headers=..., timeout=15000)
      - OLD: fetch_json(p, url, form, timeout_ms=..., extra_headers=...)
    - 서버가 text/* 로 내려도 본문이 JSON처럼 생기면 강제 파싱
    """
    import json

    # 하위호환 키 매핑
    if "timeout_ms" in kwargs and "timeout" not in kwargs:
        kwargs["timeout"] = kwargs.pop("timeout_ms")
    if "extra_headers" in kwargs and "headers" not in kwargs:
        kwargs["headers"] = kwargs.pop("extra_headers")

    # 위치 인자(구버전) → 신버전 정규화
    if args and isinstance(args[0], str) and "url" not in kwargs:
        kwargs["url"] = args[0]
        if len(args) >= 2 and "data" not in kwargs:
            kwargs["data"] = args[1]
        if "method" not in kwargs:
            kwargs["method"] = "POST" if ("data" in kwargs and kwargs["data"] is not None) else "GET"

    method  = (kwargs.get("method") or "GET").upper()
    url     = kwargs["url"]
    data    = kwargs.get("data")
    headers = dict(kwargs.get("headers") or {})
    timeout = float(kwargs.get("timeout") or 10000)

    # 기본 헤더
    base_hdrs = {"Accept": "application/json, text/plain, */*", "X-Requested-With": "XMLHttpRequest"}
    base_hdrs.update(headers or {})

    # RequestContext
    req = _request_ctx(scope_or_page)

    # fetch 인자
    fetch_kwargs = {"method": method, "headers": base_hdrs, "timeout": timeout}
    if data is not None:
        fetch_kwargs["data"] = data

    # 호출
    resp = await req.fetch(url, **fetch_kwargs)
    txt = await resp.text()
    if resp.status < 200 or resp.status >= 300:
        raise RuntimeError(f"{resp.status} {url} — {txt[:200]}")

    # 안전 파싱
    ctype = (resp.headers.get("content-type") or "").lower()
    def _looks_like_json(s: str) -> bool:
        s = (s or "").lstrip()
        return s.startswith("{") or s.startswith("[")
    if "application/json" in ctype or _looks_like_json(txt):
        try:
            return json.loads(txt)
        except Exception:
            return txt  # 최후엔 원문
    return txt

async def safe_click(el) -> bool:
    try: await el.click(timeout=CLICK_TIMEOUT); return True
    except: return False

# 3) 버튼 클릭 보강 (click_like 함수 교체)
async def click_like(scope, rx):
    # 1) role=button 텍스트 매칭 (우선 시도)
    try:
        btn = scope.get_by_role("button", name=rx)
        if await btn.count():
            el = btn.first
            try: await el.scroll_into_view_if_needed()
            except: pass
            try:
                await el.click(timeout=800)
                await scope.wait_for_timeout(200)
                return True
            except: pass
    except: pass

    # 2) id/class 시그니처
    for sel in ["#btnNext", ".btn-next", "button.next", "a.next"]:
        try:
            loc = scope.locator(sel)
            if await loc.count():
                el = loc.first
                try: await el.scroll_into_view_if_needed()
                except: pass
                try:
                    await el.click(timeout=800)
                    await scope.wait_for_timeout(200)
                    return True
                except: pass
        except: pass

    # 3) 텍스트 포함 후보 전수 스캔 + JS 강제 클릭
    try:
        loc = scope.locator("button, a, [role='button'], input[type=button], input[type=submit]").filter(has_text=rx)
        if await loc.count():
            el = loc.first
            eh = await el.element_handle()
            try: await el.scroll_into_view_if_needed()
            except: pass
            try:
                await el.click(timeout=800)
                await scope.wait_for_timeout(200)
                return True
            except: pass
            try:
                await scope.evaluate("(el)=>{ try{ el.click(); }catch(e){} }", eh)
                await scope.wait_for_timeout(200)
                return True
            except: pass
    except: pass
    return False


# === sdCode별 좌석 집계 (세션 전환 독립 헬퍼 버전) ============================
from collections import Counter

def _z3(x): return str(x).zfill(3)

@trace_step("load_all_schedules")
async def _load_all_schedules(pop):
    """
    /prod 호출로 listSch를 받아:
      - seq2code: sdSeq -> sdCode
      - code2seq: sdCode(3자리) -> sdSeq
      - seq2date: sdSeq -> perfDate(yyyymmdd)
      - 공통 파라미터(prodSeq, chnlCd, saleTycd, saleCondNo, csrfToken)
    """
    # DOM에서 기본 파라미터
    prodSeq   = await pop.evaluate("document.querySelector('#prodSeq')?.value || null")
    chnlCd    = (await pop.evaluate("document.querySelector('#chnlCd')?.value || 'WEB'")) or "WEB"
    saleTycd  = (await pop.evaluate("document.querySelector('#saleTycd')?.value || 'SALE_NORMAL'")) or "SALE_NORMAL"
    saleCond  = (await pop.evaluate("document.querySelector('#saleCondNo')?.value || '1'")) or "1"
    perfDate0 = (await pop.evaluate("document.querySelector('#perfDate')?.value || ''")) or ""
    csrfToken = (await pop.evaluate("document.querySelector('#csrfToken')?.value || ''")) or ""
    if not prodSeq:
        # 팝업 초기엔 빈 값일 수 있음. window.prodData에서 재시도.
        for _ in range(12):
            try:
                prodSeq = await pop.evaluate("window.prodData && window.prodData.prodSeq || null")
            except: prodSeq = None
            if prodSeq: break
            await pop.wait_for_timeout(500)
    # ➕ Referer 준비
    try:
        sd_first = await pop.evaluate("document.querySelector('#sdSeq')?.value || ''")
    except:
        sd_first = ""
    REFS = build_onestop_referers(pop, str(prodSeq), str(sd_first or ""))
    ORI  = _onestop_origin_from_context(pop)
    H_RS = {"Referer": REFS["rs"], "Origin": ORI}  # ➕ Origin

    js = await post_api(pop, "/rs/prod", {
        "prodSeq": prodSeq, "chnlCd": chnlCd, "sdSeq": "",
        "saleTycd": saleTycd, "saleCondNo": saleCond,
        "perfDate": perfDate0, "csrfToken": csrfToken
    }, extra_headers=H_RS)
    # normalize potential string payload to dict
    import json as _json
    if isinstance(js, str):
        s = js.strip()
        if s and s[0] in '[{':
            try: js = _json.loads(s)
            except Exception: js = {}
        else:
            js = {}
    if not isinstance(js, dict):
        js = {}
    sch = js.get("listSch") or []
    seq2code, code2seq, seq2date = {}, {}, {}
    for it in _iter_dicts(sch):
        raw_code = (it.get("sdCode") or it.get("sd_code") or it.get("schCode") or it.get("scheduleCode") or "")
        raw_seq  =  it.get("sdSeq")  or it.get("sd_seq")  or it.get("sdSeqNo")  or it.get("sdNo")  or it.get("schSeq")
        if not raw_seq:
            continue
        seq  = int(raw_seq)
        # ★ sdCode 비는 케이스 방어: 없으면 sdSeq 를 3자리로 사용
        code = (str(raw_code).strip() or str(seq)).zfill(3)

        seq2code[seq] = code
        code2seq[code] = seq
        perf = it.get("sdStartDt") or it.get("sdStartDay") or it.get("perfStartDay") or ""
        # ➕ 날짜 정규화: yyyy-mm-dd → yyyymmdd
        perf_s = str(perf or "")
        if "-" in perf_s:
            perf_s = perf_s.replace("-", "")
        perf_s = re.sub(r"[^0-9]", "", perf_s)[:8]  # 혹시 모를 노이즈 제거
        seq2date[seq] = perf_s
    return str(prodSeq), chnlCd, saleTycd, saleCond, csrfToken, seq2code, code2seq, seq2date
# === PATCH [swap_session_to_sdseq] ===========================================
@trace_step("swap_session_to_sdseq")
async def _swap_session_to_sdseq(pop, prodSeq, sdSeq,
                                 chnlCd="WEB", saleTycd="SALE_NORMAL",
                                 saleCondNo="1", perfDate="", csrfToken="",
                                 extra_headers=None):

    pack = await ensure_full_rs_params(pop, str(prodSeq), str(sdSeq))
    perfDate  = perfDate  or pack["perfDate"]
    csrfToken = csrfToken or pack["csrfToken"]

    REFS = build_onestop_referers(pop, str(prodSeq), str(sdSeq))
    ORI  = _onestop_origin_from_context(pop)
    H_RS = {"Referer": REFS["rs"], "Origin": ORI}
    if extra_headers: H_RS.update(extra_headers or {})

    def P(path, form):
        base = {"langCd": "ko", "csrfToken": csrfToken}
        base.update(form)
        return post_api(pop, f"/rs/{path}", base, extra_headers=H_RS)

    await P("prodChk",      {"prodSeq": prodSeq, "sdSeq": sdSeq, "chnlCd": chnlCd, "saleTycd": saleTycd, "saleCondNo": saleCondNo, "perfDate": perfDate,
                             "user_member_info1": "", "user_member_info2": "", "enCryptTelNo": ""})
    await P("chkProdSdSeq", {"prodSeq": prodSeq, "sdSeq": sdSeq, "chnlCd": chnlCd})
    try:
        await P("informLimit", {"prodSeq": prodSeq, "sdSeq": sdSeq, "chnlCd": chnlCd, "saleTycd": saleTycd, "saleCondNo": saleCondNo})
    except Exception as e:
        dlog(f"[SWAP] informLimit warn: {e}")
# ============================================================================ #


@trace_step("fetch_seat_status_list")
async def _fetch_seat_status_list(pop, prodSeq, sdSeq, chnlCd="WEB", csrfToken="", extra_headers=None):  # ➕
    # 1) snake_case
    try:
        js = await post_api(pop, "/seat/GetRsSeatStatusList", {
            "prod_seq": str(prodSeq), "sd_seq": str(sdSeq), "chnl_cd": chnlCd,
            "timeStemp": "", "csrfToken": csrfToken or ""
        }, extra_headers=extra_headers)  # ➕
        if isinstance(js, dict) and js.get("resultCode") not in (None, "0000"):
            dlog(f"[SEAT] snake_case non-0000: {js.get('resultCode')} {js.get('resultMessage')}")
        lst = (js.get("list") if isinstance(js, dict) else js) or []
        if isinstance(lst, list) and lst:
            return lst
    except Exception as e:
        dlog(f"[SEAT] snake_case failed: {e}")

    # 2) camelCase
    try:
        js = await post_api(pop, "/seat/GetRsSeatStatusList", {
            "prodSeq": str(prodSeq), "sdSeq": str(sdSeq), "chnlCd": chnlCd,
            "timeStemp": "", "csrfToken": csrfToken or ""
        }, extra_headers=extra_headers)  # ➕
        if isinstance(js, dict) and js.get("resultCode") not in (None, "0000"):
            dlog(f"[SEAT] camelCase non-0000: {js.get('resultCode')} {js.get('resultMessage')}")
        lst = (js.get("list") if isinstance(js, dict) else js) or []
        return lst if isinstance(lst, list) else []
    except Exception as e:
        dlog(f"[SEAT] camelCase failed: {e}")
        return []

# === REPLACE ENTIRE FUNCTION: _count_seats ===
def _count_seats(lst, available_codes={"SS01000", "SS02000", "SS03000"}):
    total, remain, by = 0, 0, {}
    for it in (lst or []):
        # ★ 핵심 가드: dict 아니면 스킵 (str/None/숫자/리스트 조각 등 전부 무시)
        if not isinstance(it, dict):
            continue
        use = (it.get("useYn") or it.get("use_yn") or "N")
        if use != "Y":
            continue
        st  = (it.get("seatStatusCd") or it.get("seat_status_cd") or it.get("statusCd") or "")
        try:
            cnt = int(it.get("seatCnt") or it.get("seat_cnt") or 0)
        except:
            cnt = 0
        total += cnt
        by[st] = by.get(st, 0) + cnt
        if st in available_codes:
            remain += cnt
    return total, remain, by


@trace_step("seat_counts_by_codes_v4")
async def seat_counts_by_codes_v4(page_or_pop, codes_filter=None):
    # 0) filmonestop 스코프 확보
    scope = await ensure_filmonestop_scope(page_or_pop)

    # 1) 모든 회차 로드
    prodSeq, chnlCd, saleTycd, saleCond, csrfToken, seq2code, code2seq, seq2date = await _load_all_schedules(scope)

    results_all = {}

    # 2) 각 회차: 세션 전환 → 좌석 상태
    for sdSeq, code in seq2code.items():
        perf = seq2date.get(sdSeq, "")

        # referer / origin 구성
        REFS   = build_onestop_referers(scope, str(prodSeq), str(sdSeq))
        ORI    = _onestop_origin_from_context(scope)
        H_RS   = {"Referer": REFS["rs"],   "Origin": ORI}
        H_SEAT = {"Referer": REFS["seat"], "Origin": ORI}

        # 🔸 쿠키/오리진 워밍(중요) — RS 401/500 완화
        try:
            await ensure_onestop_cookies(scope, str(prodSeq), str(sdSeq))
        except Exception as e:
            dlog(f"[COOKIES] warm failed sdCode={code} sdSeq={sdSeq}: {e}")

        # 세션 스왑
        try:
            await _swap_session_to_sdseq(
                scope, prodSeq, sdSeq,
                chnlCd=chnlCd, saleTycd=saleTycd,
                saleCondNo=saleCond, perfDate=perf, csrfToken=csrfToken,
                extra_headers=H_RS
            )
        except Exception as e:
            dlog(f"[SESS] swap failed sdCode={code} sdSeq={sdSeq}: {e}")

        # 좌석 상태 조회(정공법)
        lst = await _fetch_seat_status_list(
            scope, prodSeq, sdSeq,
            chnlCd=chnlCd, csrfToken=csrfToken,
            extra_headers=H_SEAT
        )
        lst = _normalize_seat_list(lst)   
        # 2-1) 정상 케이스
        if isinstance(lst, list) and lst:
            total, remain, by = _count_seats(lst, available_codes=AVAILABLE_CODES)
            results_all[code] = {"prodSeq": prodSeq, "sdSeq": sdSeq, "total": total, "remain": remain, "by_status": by}
            continue

        # 2-2) 🔁 폴백: 베이스맵/존 맵으로 집계(지정석/ZONE 설계에서만 가능)
        try:
            total, remain, by, plan = await seat_summary_zone_only(scope, prodSeq, sdSeq, chnlCd=chnlCd, saleTycd=saleTycd)
            results_all[code] = {"prodSeq": prodSeq, "sdSeq": sdSeq, "total": total, "remain": remain, "by_status": by, "plan": plan}
        except Exception as e:
            dlog(f"[SEAT] fallback zone summary failed sdCode={code} sdSeq={sdSeq}: {e}")
            results_all[code] = {"prodSeq": prodSeq, "sdSeq": sdSeq, "total": 0, "remain": 0, "by_status": {}}

    # 3) 필터링
    if codes_filter:
        wanted = {str(c).zfill(3) for c in codes_filter}
        return {c: v for c, v in results_all.items() if c in wanted}
    return results_all

# ---------------- 진입 ----------------
@trace_step("open_booking_from_resmain")
async def open_booking_from_resmain(p: Page) -> Optional[Page]:
    if p.is_closed(): return None

    async def _click_candidates() -> bool:
        el = p.locator("a[href*='/booking' i], [onclick*='openOnestop' i], [onclick*='booking' i]")
        cnt = await el.count()
        if cnt:
            for i in range(cnt):
                it = el.nth(i)
                href = (await it.get_attribute("href") or "")
                t = (await it.inner_text() or "").strip()
                if "mypage/tickets/list" in href or RX_ANTI.search(t): continue
                if await safe_click(it): return True
        return await click_like(p, RX_RESERVE)

    try:
        async with p.expect_popup() as pinfo:
            ok = await _click_candidates()
            if not ok:
                ok2 = await _click_candidates()
                if not ok2: return p if not p.is_closed() else None
        pop = await pinfo.value
        try:
            await pop.bring_to_front()
            await pop.evaluate("window.focus()")
        except:
            pass
        await drive_price_inside_iframe(pop)
        try:
            stats = await seat_counts_by_codes_v4(pop, SD_CODES)
            for k, v in stats.items():
                dlog(f"[SEAT] sdCode={k} sdSeq={v['sdSeq']} total={v['total']} remain={v['remain']} by={v['by_status']}")

        except Exception as e:
            import traceback as _tb; dlog(f"[SEAT] seat count failed: {e} :: {e.__class__.__name__}"); dlog(_tb.format_exc().splitlines()[-1] if _tb.format_exc() else '')
        try:
            await pop.wait_for_load_state("domcontentloaded", timeout=STEP_TIMEOUT)
            await pop.wait_for_url(re.compile(r"^https?://"), timeout=STEP_TIMEOUT)
        except: pass
        return pop if not pop.is_closed() else None
    except:
        if await _click_candidates():
            try: await p.wait_for_load_state("domcontentloaded", timeout=STEP_TIMEOUT)
            except: pass
            return p if not p.is_closed() else None
        return p if not p.is_closed() else None
# --- 예약 스코프/단계 감지 & /price 처리 ---

async def find_booking_scope(p: Page) -> Optional[Union[Page, Frame]]:
    """filmonestop 쪽에서 동작할 scope(Page or Frame) 리턴"""
    if p.is_closed():
        return None
    for f in p.frames:
        if "filmonestop.maketicket.co.kr" in (f.url or ""):
            return f
    if "filmonestop.maketicket.co.kr" in (p.url or ""):
        return p
    return None

async def is_price_page(scope) -> bool:
    if scope is None: 
        return False
    try:
        return (
            await scope.locator("input[type=radio][name='tkttypSeq']").count() > 0 or
            await scope.locator("select[name*='tkttyp']").count() > 0 or
            await scope.locator("select[name='rsVolume'], input[name='rsVolume']").count() > 0 or
            await scope.get_by_text(re.compile(r"(가격|티켓(유형|종류)|일반|성인|청소년)")).count() > 0
        )
    except:
        return False

async def is_zone_page(scope) -> bool:
    if not scope:
        return False
    try:
        if await scope.locator(".zone, [data-zone], .block-summary, text=존 선택").count():
            return True
    except Exception:
        pass
    return False

async def is_seat_page(scope) -> bool:
    if not scope:
        return False
    try:
        if await scope.locator(".seat, [data-seat-state], g.seat, rect.seat").count():
            return True
    except Exception:
        pass
    return False

# ---------------- 좌석/가용 수집 ----------------
async def find_seat_frame(p: Page) -> Optional[Frame]:
    if p.is_closed():
        return None
    for f in p.frames:
        u = (f.url or "").lower()
        # 좌석맵 전용 경로만 허용 (/price, /zone 은 제외)
        if "filmonestop.maketicket.co.kr" in u and re.search(r"/seat|rsseat|seatmap|getrsseatbasemap", u):
            try:
                # 진짜 좌석 엘리먼트가 있어야 좌석맵으로 인정
                if await f.locator(".seat, [data-seat-state], g.seat, rect.seat").count() > 0:
                    return f
            except:
                # DOM 확인 실패 시에도 일단 후보로
                return f
    return None

def count_avail_in_zone(zone_js: Any) -> Tuple[int,int]:
    """
    다양한 스키마에 맞춰 '전체'와 '가용' 좌석 수를 추정.
    seat 객체 안에서 가능한 키들을 폭넓게 체크해서 Y/available/0 등으로 판정.
    """
    total = 0
    avail = 0
    seats = []
    # seats 후보 경로
    for key in ["seatList","seats","list","items","data","seat_map","seatMap","zones"]:
        v = zone_js.get(key) if isinstance(zone_js, dict) else None
        if isinstance(v, list): seats = v; break
    if not seats and isinstance(zone_js, list):
        seats = zone_js
    for s in _iter_dicts(seats):
        total += 1
        state = ""
        for k in ["state","seatState","seatStateCd","useYn","able","avail","isAvailable","st"]:
            v = s.get(k) if isinstance(s, dict) else None
            if v is None: continue
            state = str(v).lower()
            break
        seat_ok = False
        if state in ("y","yes","true","available","able","0"): seat_ok = True
        if isinstance(s, dict):
            if s.get("disabled") is False: seat_ok = True
            if s.get("status") in ("Y","AVAILABLE","OK"): seat_ok = True
        if seat_ok: avail += 1
    return total, avail

@trace_step("availability_report")
async def availability_report(p: Page, params: Dict[str,str]) -> Dict[str,Any]:
    """좌석/잔여 종합 로깅용 데이터 수집"""
    prodSeq = params.get("prodSeq"); sdSeq = params.get("sdSeq"); perfDate = params.get("perfDate")
    csrf = params.get("csrfToken"); ch = params.get("chnlCd","WEB")

    def common(extra: Dict[str,Any]={}):
        base = {"prodSeq": prodSeq, "sdSeq": sdSeq, "chnlCd": ch, "csrfToken": csrf}
        base.update(extra); return base

    out: Dict[str,Any] = {"zones":[]}

    # 1) prodSummary (공연 요약) – 잔여/판매상태 힌트
    try:
        js = await fetch_json(p, "https://filmonestopapi.maketicket.co.kr/api/v1/rs/prodSummary",
                              common({"perfDate": perfDate}))
        out["prodSummary"] = js
    except Exception as e:
        out["prodSummary_error"] = str(e)

    # 2) blockSummary2 (존/블록 단위 요약) – 흔히 zone별 잔여가 들어옴
    try:
        js = await fetch_json(p, "https://filmonestopapi.maketicket.co.kr/api/v1/rs/blockSummary2",
                              common({"perfDate": perfDate}))
        out["blockSummary2"] = js
    except Exception as e:
        out["blockSummary2_error"] = str(e)

    # 3) 좌석맵 베이스
    try:
        base_map = await fetch_json(p, "https://filmonestopapi.maketicket.co.kr/api/v1/rs/seat/GetRsSeatBaseMap",
                                    {"prod_seq": prodSeq, "sd_seq": sdSeq, "chnl_cd": ch, "sale_tycd":"SALE_NORMAL"})
        out["baseMap"] = base_map
        # zone id 추출
        zone_ids = []
        if isinstance(base_map, dict):
            for key in ["zoneList","zones","list","items","data"]:
                v = base_map.get(key)
                if isinstance(v, list):
                    for z in _iter_dicts(v):
                        zid = z.get("zone_id") or z.get("zoneId") or z.get("id")
                        if zid: zone_ids.append(zid)
        # 4) 존별 상세 맵 – 전체/가용 계산
        zsum = []
        for zid in zone_ids[:12]:  # 과도한 호출 방지
            try:
                zj = await fetch_json(p, "https://filmonestopapi.maketicket.co.kr/api/v1/rs/seat/GetRsZoneSeatMapInfo",
                                      {"prod_seq": prodSeq, "sd_seq": sdSeq, "chnl_cd": ch,
                                       "zone_id": zid, "sale_tycd":"SALE_NORMAL", "timeStemp": ""})
                tot, av = (0,0)
                if isinstance(zj, dict):
                    tot, av = count_avail_in_zone(zj)
                zsum.append({"zoneId": zid, "total": tot, "available": av})
            except Exception as e:
                zsum.append({"zoneId": zid, "error": str(e)})
        out["zones"] = zsum
        # 총합
        out["totalSeats"] = sum((z.get("total",0) if isinstance(z, dict) else 0) for z in zsum)
        out["availableSeats"] = sum((z.get("available",0) if isinstance(z, dict) else 0) for z in zsum)
        out["soldSeats"] = max(0, out.get("totalSeats",0) - out.get("availableSeats",0))
        # ... 기존 zones 합산 로직 뒤
        if "totalSeats" not in out:
            TR.warn("avail.no_total", hint="seat map/zone api missing or blocked")
        else:
            TR.ev("avail", total=out["totalSeats"], avail=out["availableSeats"], sold=out["soldSeats"], zones=len(out.get("zones",[])))

    except Exception as e:
        out["baseMap_error"] = str(e)

    return out


async def pick_any_seat(scope):
    """좌석맵에서 가용 좌석 하나 찍기."""
    try:
        seat = scope.locator(
            ".seat.available, [data-seat-state='A'], g.seat.available, rect.seat.available"
        ).first
        if await seat.count():
            await seat.click(timeout=800)
            await scope.wait_for_timeout(200)
            return True
    except Exception:
        pass
    return False

def attach_payment_autonav(p: Page) -> None:
    def _on_req(req):
        try:
            url = req.url or ""
            if PAYMENT_HINT_RX.search(url):
                # 일부 화면은 XHR만 날리고 라우팅을 안 바꾼다 → 힌트 감지 시 성공 판정에 사용
                pass
        except: pass
    p.on("request", _on_req)

# 4) 수량=1 세팅에 number 인풋/커스텀 위젯 보강 (ensure_qty_one 일부 추가)
@trace_step("ensure_qty_one")
async def ensure_qty_one(scope):
    # (A) 스샷 케이스: id 고정 (#volume_1_1)
    try:
        v = scope.locator("#volume_1_1")
        if await v.count():
            await v.first.select_option("1")
            # onchange 핸들러(slTicket)를 깨우기 위한 이벤트 강발
            eh = await v.first.element_handle()
            await scope.evaluate("(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true}));}", eh)
            await scope.wait_for_timeout(120)
            return True
    except:
        pass

    # (a) 일반 select 우선
    selectors = [
        "select[name='rsVolume']",
        "#rsVolume",
        "select[name='selectedTicketCount']",
        "#selectedTicketCount",
        "select[name*='count' i]",
        "#sellCnt",
    ]
    for sel in selectors:
        try:
            loc = scope.locator(sel)
            if await loc.count():
                try:
                    await loc.first.select_option("1")
                    # 이벤트 강발 (사이트에 따라 필요)
                    try:
                        eh = await loc.first.element_handle()
                        await scope.evaluate("(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true}));}", eh)
                    except: pass
                    await scope.wait_for_timeout(120)
                    return True
                except: pass
        except: pass

    # (b) number 입력형
    try:
        num = scope.locator("input[type='number'][name='rsVolume'], input[type='number'][name='selectedTicketCount']")
        if await num.count():
            await num.first.fill("1")
            await scope.wait_for_timeout(120)
            return True
    except: pass

    # (c) 커스텀 드롭다운 (react-select/AntD 등)
    try:
        openers = ["[role='combobox']", ".ant-select-selector", ".select__control",
                   ".dropdown-toggle", ".cm-select", ".custom-select"]
        for op_sel in openers:
            op = scope.locator(op_sel)
            if await op.count():
                await op.first.click(timeout=400)
                opt = scope.locator("[role='option'] >> text=/^\\s*1\\s*$/")
                if not await opt.count():
                    opt = scope.locator("li,div,button").filter(has_text=re.compile(r"^\\s*1\\s*$"))
                if await opt.count():
                    await opt.first.click(timeout=400)
                    await scope.wait_for_timeout(120)
                    return True
    except: pass

    # (d) 마지막: 히든필드 직접 세팅 + 이벤트
    try:
        ok = await scope.evaluate("""
            () => {
              const fire = (el,t)=>el&&el.dispatchEvent(new Event(t,{bubbles:true}));
              const set = (name,val)=>{
                const el = document.querySelector(`[name="${name}"]`) || document.getElementById(name);
                if(!el) return false;
                el.value = val; fire(el,'input'); fire(el,'change'); return true;
              };
              return set('selectedTicketCount','1') || set('rsVolume','1');
            }
        """)
        if ok:
            await scope.wait_for_timeout(100)
            return True
    except: pass

    return False

# === 가격화면 강제 동작: qty=1 설정 + Next 클릭 ====================
@trace_step("enter_price_and_next")
async def enter_price_and_next(popup_or_scope):
    # scope가 Frame/Locator/Popup 무엇이든 대응
    scope = popup_or_scope
    page = getattr(scope, "page", None) or getattr(scope, "context", None) or getattr(scope, "owner", None)
    page = getattr(scope, "page", None) or getattr(scope, "context", None) or getattr(scope, "owner", None)
    page = getattr(scope, "page", None) or getattr(scope, "context", None) or getattr(scope, "owner", None)
    try:
        # 팝업 포커스 보장
        if hasattr(scope, "bring_to_front"):
            await scope.bring_to_front()
        if hasattr(scope, "evaluate"):
            await scope.evaluate("window.focus && window.focus()")
    except: pass

    # (0) 프레임/스코프 정규화
    frame = getattr(scope, "main_frame", None) or getattr(scope, "frame", None) or scope
    try:
        # 가격 섹션 앵커 보이길 대기
        await frame.wait_for_selector("#partTicketType, table.table-price, select[id^='volume_']", timeout=8000)
    except: pass

    # (1) qty=1 세팅 시도(여러 경로로 전부)
    async def _set_qty_1():
        # 1-1) id 직격 (네 스샷 기준)
        sel = frame.locator("#volume_1_1")
        if await sel.count():
            try:
                # value=1 시도
                try:   await sel.first.select_option("1")
                except: await sel.first.select_option(index=1)   # 두 번째 옵션(=1매)
                # onchange(slTicket) 깨우기
                eh = await sel.first.element_handle()
                await frame.evaluate("(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true})); if(window.slTicket){try{slTicket(el,'1','1','null','');}catch(e){}}}", eh)
                await frame.wait_for_timeout(150)
                return True
            except: pass

        # 1-2) '일반' 행 안의 volume_* 탐색
        try:
            row = frame.locator("tr", has=frame.get_by_text("일반", exact=True))
            if await row.count():
                s2 = row.locator("select[id^='volume_']")
                if await s2.count():
                    try:
                        try:   await s2.first.select_option("1")
                        except: await s2.first.select_option(index=1)
                        eh = await s2.first.element_handle()
                        await frame.evaluate("(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true})); if(window.slTicket){try{slTicket(el,'1','1','null','');}catch(e){}}}", eh)
                        await frame.wait_for_timeout(150)
                        return True
                    except: pass
        except: pass

        # 1-3) 다른 select 후보 전수
        for css in ["select[name='rsVolume']","#rsVolume","select[name='selectedTicketCount']","#selectedTicketCount","select[id^='volume_']"]:
            try:
                loc = frame.locator(css)
                if await loc.count():
                    try:
                        try:   await loc.first.select_option("1")
                        except: await loc.first.select_option(index=1)
                        eh = await loc.first.element_handle()
                        await frame.evaluate("(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true})); if(window.slTicket){try{slTicket(el,'1','1','null','');}catch(e){}}}", eh)
                        await frame.wait_for_timeout(150)
                        return True
                    except: pass
            except: pass

        # 1-4) 마지막: 히든필드 직업 + 이벤트
        try:
            ok = await frame.evaluate("""()=>{
                const fire=(el,t)=>el&&el.dispatchEvent(new Event(t,{bubbles:true}));
                const set=(name,val)=>{
                    const el=document.querySelector(`[name="${name}"]`)||document.getElementById(name);
                    if(!el) return false; el.value=val; fire(el,'input'); fire(el,'change'); return true;
                };
                return set('selectedTicketCount','1')||set('rsVolume','1');
            }""")
            if ok:
                await frame.wait_for_timeout(100)
                return True
        except: pass
        return False

    qty_ok = await _set_qty_1()

    # (2) Next 버튼 활성화 되도록 방해 요소 제거/우회
    try:
        # 흔한 dim/overlay 제거
        await frame.evaluate("""()=>{
            for (const sel of ['.dim','.__dim__','.overlay','.modal-backdrop','.loading']){
                const el=document.querySelector(sel); if(el) el.style.display='none';
            }
        }""")
    except: pass

    # (3) Next 강제 클릭 (disabled 우회 + JS 강클릭)
    async def _try_next():
        # 후보 셀렉터
        for sel in ["#btnNext",".btn-next","button.next","a.next","button:has-text('다음')","[id*='Next' i]"]:
            try:
                loc = frame.locator(sel)
                if not await loc.count(): 
                    continue
                el = loc.first
                try: await el.scroll_into_view_if_needed()
                except: pass

                # disabled 우회
                try:
                    # is_disabled() 안 먹는 경우가 많아 직접 attr 제거
                    eh = await el.element_handle()
                    await frame.evaluate("(el)=>{el.removeAttribute && el.removeAttribute('disabled'); el.classList && el.classList.remove('disabled');}", eh)
                except: pass

                # 클릭 → 실패 시 JS 강클릭
                try:
                    await el.click(timeout=1000)
                    return True
                except:
                    try:
                        eh = await el.element_handle()
                        await frame.evaluate("(el)=>{try{el.click();}catch(e){}}", eh)
                        return True
                    except: pass
            except: pass
        # 마지막: Enter 키
        try:
            await frame.keyboard.press("Enter")
            return True
        except: pass
        return False

    next_ok = await _try_next()

    # (4) 네트워크 힌트(선택): qty 반영되면 종종 아래 api가 연쇄로 나감
    # (로그 확인용 — 실패해도 무시)
    try:
        await frame.wait_for_timeout(100)
    except: pass

    return bool(qty_ok and next_ok)


# === 가격화면: iframe 안에서 qty=1 → 다음 강제 ===
async def _get_price_frame(popup):
    # 아이프레임 붙을 때까지
    iframe_el = await popup.wait_for_selector("#oneStopFrame", state="attached", timeout=15000)
    frame = await iframe_el.content_frame()
    # 프레임이 /price 로딩될 때까지
    try:
        await frame.wait_for_url(lambda url: "/price" in url or "/seat" in url or "/zone" in url, timeout=15000)
    except:
        pass
    await frame.wait_for_load_state("domcontentloaded")
    return frame

async def _set_qty_1_in_frame(frame):
    # 1) 네가 본 셀렉터: #volume_1_1
    sel = frame.locator("#volume_1_1")
    if await sel.count():
        try:
            try:
                await sel.first.select_option("1")
            except:
                await sel.first.select_option(index=1)  # 두 번째 옵션이 1매일 때
            eh = await sel.first.element_handle()
            # 사이트가 slTicket(change)만 듣는 구조라 이벤트+핸들러 직접 호출
            await frame.evaluate(
                "(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));"
                "el.dispatchEvent(new Event('change',{bubbles:true}));"
                "if(window.slTicket){try{slTicket(el,'1','1','null','');}catch(e){}}}",
                eh
            )
            await frame.wait_for_timeout(150)
            return True
        except:
            pass

    # 2) 백업: '일반' 행 안의 select[id^=volume_]
    try:
        row = frame.locator("tr", has=frame.get_by_text("일반", exact=True))
        if await row.count():
            s2 = row.locator("select[id^='volume_']")
            if await s2.count():
                try:
                    try:
                        await s2.first.select_option("1")
                    except:
                        await s2.first.select_option(index=1)
                    eh = await s2.first.element_handle()
                    await frame.evaluate(
                        "(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));"
                        "el.dispatchEvent(new Event('change',{bubbles:true}));"
                        "if(window.slTicket){try{slTicket(el,'1','1','null','');}catch(e){}}}",
                        eh
                    )
                    await frame.wait_for_timeout(150)
                    return True
                except:
                    pass
    except:
        pass

    # 3) 전수 후보
    for css in ["select[name='rsVolume']", "#rsVolume",
                "select[name='selectedTicketCount']", "#selectedTicketCount",
                "select[id^='volume_']"]:
        try:
            loc = frame.locator(css)
            if await loc.count():
                try:
                    try:
                        await loc.first.select_option("1")
                    except:
                        await loc.first.select_option(index=1)
                    eh = await loc.first.element_handle()
                    await frame.evaluate(
                        "(el)=>{el.dispatchEvent(new Event('input',{bubbles:true}));"
                        "el.dispatchEvent(new Event('change',{bubbles:true}));"
                        "if(window.slTicket){try{slTicket(el,'1','1','null','');}catch(e){}}}",
                        eh
                    )
                    await frame.wait_for_timeout(150)
                    return True
                except:
                    pass
        except:
            pass

    # 4) 최후: 히든필드 직접 세팅
    try:
        ok = await frame.evaluate("""()=>{
            const fire=(el,t)=>el&&el.dispatchEvent(new Event(t,{bubbles:true}));
            const set=(name,val)=>{
              const el=document.querySelector(`[name="${name}"]`)||document.getElementById(name);
              if(!el) return false; el.value=val; fire(el,'input'); fire(el,'change'); return true;
            };
            return set('selectedTicketCount','1')||set('rsVolume','1');
        }""")
        if ok:
            await frame.wait_for_timeout(100)
            return True
    except:
        pass
    return False

async def _click_next_in_frame(frame):
    # 오버레이/disabled 제거
    try:
        await frame.evaluate("""()=>{
            for(const s of ['.dim','.__dim__','.overlay','.modal-backdrop','.loading']){
              const e=document.querySelector(s); if(e) e.style.display='none';
            }
        }""")
    except:
        pass

    # 버튼 클릭(여러 후보 + 강클릭)
    candidates = ["#btnNext", ".btn-next", "button.next", "a.next",
                  "button:has-text('다음')", "[id*='Next' i]"]
    for css in candidates:
        try:
            loc = frame.locator(css)
            if not await loc.count():
                continue
            el = loc.first
            try:
                await el.scroll_into_view_if_needed()
            except:
                pass
            try:
                # disabled 속성 제거
                eh = await el.element_handle()
                await frame.evaluate(
                    "(el)=>{try{el.removeAttribute('disabled');el.classList&&el.classList.remove('disabled');}catch(e){}}",
                    eh
                )
            except:
                pass
            try:
                await el.click(timeout=1200)
                return True
            except:
                try:
                    eh = await el.element_handle()
                    await frame.evaluate("(el)=>{try{el.click();}catch(e){}}", eh)
                    return True
                except:
                    pass
        except:
            pass

    # 마지막: Enter 키
    try:
        await frame.keyboard.press("Enter")
        return True
    except:
        pass
    return False

async def drive_price_inside_iframe(popup):
    # 포커스
    try:
        await popup.bring_to_front()
        await popup.evaluate("window.focus && window.focus()")
    except:
        pass

    frame = await _get_price_frame(popup)

    # 가격화면이 아닐 수도 있으니, 앵커가 보일 때만 진행
    try:
        await frame.wait_for_selector(
            "#partTicketType, table.table-price, select[id^='volume_']",
            timeout=8000
        )
    except:
        return False

    qty_ok  = await _set_qty_1_in_frame(frame)
    next_ok = await _click_next_in_frame(frame)
    return qty_ok and next_ok


async def force_price_qty_then_next(scope):
    """가격/티켓 유형 화면에서 qty=1로 맞추고 Next."""
    # 1) 티켓 유형 라디오/셀렉트가 있으면 1개 선택
    try:
        radios = scope.locator("input[type=radio][name='tkttypSeq']")
        if await radios.count() > 0:
            await radios.first.check(timeout=CLICK_TIMEOUT)
        else:
            selects = scope.locator("select[name*='tkttyp']")
            if await selects.count() > 0:
                await selects.first.select_option(index=0, timeout=CLICK_TIMEOUT)
    except:
        pass

    # 2) 수량 = 1 (ensure_qty_one 내부에서 volume_1_1도 직접 처리)
    ok = await ensure_qty_one(scope)

    # 3) 약관류 체크 있으면 전부 체크
    try:
        checks = scope.locator("input[type=checkbox]")
        n = await checks.count()
        for i in range(min(n, 6)):
            try: await checks.nth(i).check(timeout=300)
            except: pass
    except: pass

    # 4) Next 강제 클릭 (여러 형태 지원)
    await click_like(scope, RX_NEXT)
    return ok


# REPLACE: payment_hint_chain
@trace_step("payment_hint_chain")
async def payment_hint_chain(scope, prodSeq, sdSeq, perfDate="", csrfToken=None,
                             chnlCd="WEB", saleTycd="SALE_NORMAL", saleCondNo="1"):
    """
    자유석 결제 힌트 체인: prodChk → tickettype → informLimit → priceLimit 등
    - build_onestop_referers는 (scope, prodSeq, sdSeq) 3인자 호출
    - 모든 호출에 _ref_headers(...) 적용
    """
    # perfDate/csrf 보강
    if not perfDate:
        try:
            params = await harvest_params_from_dom(scope)
        except Exception:
            params = {}
        perfDate = params.get("perfDate", "") or ""
    if not csrfToken:
        page = getattr(scope, "page", None) or scope
        csrfToken = await ensure_csrf(page, csrfToken)

    REFS = build_onestop_referers(scope, str(prodSeq), str(sdSeq))
    H_RS  = _ref_headers(REFS, "rs")

    def P(path, form):
        base = {"langCd": "ko", "csrfToken": csrfToken}
        base.update(form)
        return post_api(scope, f"/rs/{path}", base, extra_headers=H_RS)

    try: await P("prodChk", {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
                             "chnlCd": chnlCd, "saleTycd": saleTycd,
                             "saleCondNo": saleCondNo, "perfDate": perfDate})
    except Exception as e: dlog(f"[HINT] prodChk: {e}")

    # 아래는 서비스 상황에 따라 선택 (실패해도 전체 플로우는 계속)
    for path, form in [
        ("listTicketType", {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq)}),
        ("informLimit",    {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
                            "chnlCd": chnlCd, "saleTycd": saleTycd}),
        ("priceLimit",     {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq)}),
        ("prodSummary",    {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
                            "chnlCd": chnlCd, "perfDate": perfDate}),
        ("blockSummary2",  {"prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
                            "chnlCd": chnlCd, "perfDate": perfDate}),
    ]:
        try: await P(path, form)
        except Exception as e: dlog(f"[HINT] {path}: {e}")


async def fcfs_chain_then_next(p: Page, sd_code: str, params: Dict[str,str]) -> bool:
    prodSeq = params.get("prodSeq"); sdSeq = params.get("sdSeq")
    perfDate = params.get("perfDate"); csrf = params.get("csrfToken")
    ch = params.get("chnlCd","WEB")
    seat_type = params.get("seatTypeCode", "RS")

    # 수량=1 효과
    try:
        await fetch_json(p,
            "https://filmonestopapi.maketicket.co.kr/api/v1/rs/seatStateInfo",
            {"prodSeq": prodSeq, "sdSeq": sdSeq, "seatId": "1", "csrfToken": csrf})
    except: pass

    # 변경: RS일 때만 payment_hint_chain 실행
    if seat_type == "RS":
        await payment_hint_chain(p, sd_code, dict(params))

    # Next
    clicked = await click_like(p, RX_NEXT)
    if not clicked:
        f = await find_seat_frame(p)
        if f: clicked = await click_like(f, RX_NEXT)
    return clicked

@trace_step("reached_payment")
async def reached_payment(p: Page, timeout_ms: int = PAYMENT_DETECT_TIMEOUT):
    """
    결제 도착을 오탐 없이 감지:
    - 텍스트 '결제/주문' 등은 보지 않음 (오탐 원인)
    - 모든 frame의 URL 중 '/payment' 또는 '/order' 포함일 때만 True
    - True일 때 (True, frame_url) 반환, 아니면 (False, None)
    """
    RX = re.compile(r"/(payment|order)\b", re.I)

    def scan_frames():
        try:
            for f in p.frames:
                u = (getattr(f, "url", "") or "")
                if RX.search(u):
                    return True, u
        except Exception:
            pass
        return False, None

    ok, u = scan_frames()
    if ok:
        return True, u

    end = time.time() + (timeout_ms / 1000.0)
    while time.time() < end:
        ok, u = scan_frames()
        if ok:
            return True, u
        try:
            # 네트워크 힌트도 /payment 로만 한정
            evt = await p.wait_for_event(
                "request",
                timeout=150,
                predicate=lambda r: RX.search(r.url or "") is not None,
            )
            if evt:
                # 이벤트로 감지했으면 한 번 더 프레임 스캔
                ok, u = scan_frames()
                if ok:
                    return True, u
        except:
            pass
    return False, None


# === REPLACE ENTIRE FUNCTION: step_to_payment ===
@trace_step("step_to_payment")
async def step_to_payment(work, sd_code: str, params: dict, is_fcfs: bool) -> bool:
    """
    결제 단계까지 단계별로 밀어붙이는 메인 루프.
    - filmonestop booking iframe/scope 내에서만 동작 (CORS 회피)
    - /price /zone /seat /payment 자동 감지
    - FCFS/기타면 qty=1 강제 후 Next
    """
    # A) booking scope 확보
    scope = None
    for _ in range(8):
        scope = await ensure_booking_iframe(work)
        if scope:
            break
        await click_like(work, RX_RESERVE)
        await work.wait_for_timeout(800)
    if not scope or work.is_closed():
        dlog("[ERROR] booking scope 없음 또는 창 종료")
        return False

    # B) 파라미터 보강
    params = dict(params or {})
    hp = await harvest_params_from_dom(scope)
    for k, v in (hp or {}).items():
        if not params.get(k):
            params[k] = v

    # C) 메인 루프
    start = time.time()
    while time.time() - start < STEP_TIMEOUT / 1000:
        # 결제 도착 체크
        if await reached_payment(work):
            return True

        # scope 갱신(새 iframe/팝업 보호)
        scope = (await find_booking_scope(work)) or scope

        # 단계 분기
        if await is_price_page(scope):
            dlog("[STEP] Price page → qty=1 강제 후 Next")
            await enter_price_and_next(scope)
            # ⬇⬇ 핵심: 함수 종료가 아니라 루프 지속
            continue

        elif await is_zone_page(scope):
            dlog("[STEP] Zone page → 가용 존 클릭 후 Next")
            try:
                z = scope.locator(".zone.available, .zone:not(.soldout), [data-zone-state='A']").first
                if await z.count():
                    await z.click(timeout=CLICK_TIMEOUT)
                    await scope.wait_for_timeout(300)
            except: pass
            await click_like(scope, RX_NEXT)

        elif await is_seat_page(scope):
            dlog("[STEP] Seat page → 임의 좌석 1개 선택 후 Next")
            if not await pick_any_seat(scope):
                dlog("[STEP] 좌석 선택 실패 → 매진/비공개로 판단")
                return False
            await ensure_qty_one(scope)
            await click_like(scope, RX_NEXT)

        else:
            dlog("[STEP] Unknown/FCFS-like → qty=1 + (조건부) 힌트 체인 + Next")
            TR.ev("STEP_FCFS_HINT",
                  prodSeq=str((params or {}).get("prodSeq","")),
                  sdSeq=str((params or {}).get("sdSeq") or sd_code),
                  seatType=str((params or {}).get("seatTypeCode") or "RS"))

            if params.get("sdSeq"):
                try:
                    await payment_hint_chain(
                        scope,
                        str(params.get("prodSeq", "")),
                        str(params.get("sdSeq", "")),
                        perfDate=str(params.get("perfDate", "")),
                        csrfToken=str(params.get("csrfToken", "")),
                    )
                except Exception as e:
                    dlog(f"[WARN] payment_hint_chain 예외 무시: {e}")
                    TR.warn("STEP_FCFS_HINT_SKIP_ERR", err=str(e))
            else:
                dlog("[HINT] skip payment_hint_chain (sdSeq 없음: FCFS/자유석 가능성)")

            await ensure_qty_one(scope)
            if "filmonestop.maketicket.co.kr" in (scope.url or "") and params.get("prodSeq"):
                try:
                    await payment_hint_chain(
                        scope,
                        str(params.get("prodSeq", "")),
                        str(params.get("sdSeq", "")),
                        perfDate=str(params.get("perfDate", "")),
                        csrfToken=str(params.get("csrfToken", "")),
                    )
                except Exception as e:
                    dlog(f"[WARN] payment_hint_chain 실패: {e}")
            await click_like(scope, RX_NEXT)

        # 체크박스 자동 체크(있으면)
        try:
            checks = scope.locator("input[type=checkbox]")
            n = await checks.count()
            for i in range(min(n, 8)):
                try:
                    await checks.nth(i).check(timeout=300)
                except: pass
        except: pass

        # Next 한 번 더 — 더블체크 방지
        await click_like(scope, RX_NEXT)

        # 로깅/디버깅
        scope = await find_booking_scope(work)
        if not scope or work.is_closed():
            return False  # 혹은 재시도 루프로
        await dump_ticket_counters(scope)
        scope = await find_booking_scope(work)
        if not scope or work.is_closed():
            return False
        await dump_frames_and_next(work)

        await work.wait_for_timeout(600)

    return await reached_payment(work)

# ---------------- 플로우 ----------------
async def ensure_login(ctx) -> bool:
    p = await ctx.new_page()
    await p.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=OPEN_TIMEOUT)
    log("🔐 로그인 창을 열었습니다. 로그인만 완료하세요…")
    ok = await wait_logged_in(p)
    await p.close()
    if ok: log("✅ 로그인 감지. 동시 예매 시작.")
    else:  log("❌ 로그인 감지 실패.")
    return ok

async def open_and_log_availability(work: Page, params: Dict[str,str], title: str, sd: str) -> None:
    """좌석/잔여 정보 수집 및 로그 출력"""
    try:
        rep = await availability_report(work, params)
        # 총괄
        if "totalSeats" in rep:
            log(f"🪑 [{sd}] {title} → 전체 {rep['totalSeats']} / 잔여 {rep['availableSeats']} / 매진 {rep['soldSeats']}")
        else:
            log(f"🪑 [{sd}] {title} 좌석요약 수집 실패(맵없음/권한차단). prodSummary/블록요약 일부만 표시.")
        # 존별 요약
        zones = rep.get("zones", [])
        shown = 0
        for z in zones:
            if "error" in z: continue
            log(f"   • zone {z['zoneId']}: avail {z['available']}/{z['total']}")
            shown += 1
            if shown >= 8: break
    except Exception as e:
        log(f"🪑 [{sd}] 좌석요약 오류: {e}")
# ------------- DEBUG SWITCHES -------------
DEBUG_WIRE = True   # 모든 rs/* 네트워크 요청/응답 로그
DEBUG_DOM  = True   # Next/수량 상태, 프레임 구조, 가시성/포커스

def ilog(msg: str): 
    print(msg)

def dlog(msg: str):
    try: log(msg)
    except: print(msg)

def wlog(msg: str):
    print(msg)

def elog(msg: str):
    print(msg)

def short(s: str, n=160):
    s = s or ""
    return (s[:n] + ("…(%dB)" % (len(s)-n))) if len(s) > n else s

async def attach_debuggers(p: Page):
    # 네트워크
    def _safe_short(b): 
        if not b or not TRACE_WIRE_BODY: return ""
        return (b if len(b) < 300 else b[:300] + f"...({len(b)}B)")
    p.on("request", lambda r: (
        ("maketicket.co.kr" in (r.url or "")) and
        TR.ev("wire.req", method=r.method, url=r.url, body=_safe_short(r.post_data or ""))
    ))
    async def _on_resp(resp):
        u = resp.url or ""
        if "maketicket.co.kr" in u:
            try:
                TR.ev("wire.res", status=resp.status, url=u)
            except: pass
    p.on("response", lambda r: asyncio.create_task(_on_resp(r)))
    p.on("requestfailed", lambda r: TR.warn("wire.fail", method=r.method, url=r.url, failure=str(r.failure)))
    # 콘솔/페이지 에러
    p.on("pageerror", lambda e: TR.warn("page.error", err=str(e)))
    p.on("console",   lambda m: TR.ev("console", typ=m.type, text=m.text))

async def snapshot_state(scope, tag: str, params: Dict[str,str]):
    """현재 단계/선택자/카운터/프레임/포커스 전부 JSON으로 떨굴 스냅샷"""
    try:
        page = scope if isinstance(scope, Page) else scope.page
        vis = {"visibility": None, "focus": None}
        try:
            vis = await page.evaluate("({v:document.visibilityState,f:document.hasFocus()})")
        except: pass

        # 프레임 트리
        frames = [{"name": f.name, "url": f.url} for f in page.frames]

        # 주요 선택자 매트릭스
        sels = {
            "next_btn": ["#btnNext",".btn-next","button:has-text('다음')","a:has-text('Next')"],
            "qty_select": ["select[name='rsVolume']","#rsVolume","select[name='selectedTicketCount']","#selectedTicketCount","select[name*='count' i]","#sellCnt"],
            "qty_custom": ["[role='combobox']", ".ant-select-selector",".select__control",".dropdown-toggle",".cm-select",".custom-select"],
            "price_marks": ["text=/가격|티켓유형|일반|청소년/i"],
            "zone_marks":  ["text=/존\\s*선택/","[data-zone]",".block-summary"],
            "seat_map":    ["#seatMap","canvas",".seat","[data-seat]"]
        }
        sel_state = {}
        for k, arr in sels.items():
            c = 0
            for s in arr:
                try: c += await scope.locator(s).count()
                except: pass
            sel_state[k] = c

        # 카운터들 (기존 dump_ticket_counters 기반)
        counters = {}
        try:
            counters = await scope.evaluate("""() => {
              const q = s=>document.querySelector(s);
              const val = el => el ? (el.value ?? el.textContent ?? '') : '';
              return {
                panel: (document.body.innerText.match(/선택티켓\\s*정보\\s*\\((\\d+)매\\)/)||[])[1]||'',
                sel0:  val(q('select[name*="count" i]')) || val(q('#sellCnt')) || '',
                hidden1: val(q('[name="selectedTicketCount"]')) || val(q('#selectedSeatCnt')) || '',
                hidden2: val(q('[name="rsVolume"]')) || '',
                seatId:  val(q('[name*="seatId" i], #seatId')) || ''
              };
            }""")
        except: pass

        TR.ev("snapshot", tag=tag, url=page.url, scope_url=getattr(scope, "url", None), 
              frames=frames, vis=vis, sels=sel_state, counters=counters, params=params)
        await TR.dump_html(page, f"snapshot_{tag}")
        await TR.dump_png(page, f"snapshot_{tag}")
    except Exception as e:
        TR.warn("snapshot.fail", reason=str(e))


async def dump_frames_and_next(scope) -> None:
    """프레임 구조와 Next 후보들의 상태를 자세히 덤프"""
    try:
        page = scope if isinstance(scope, Page) else scope.page
        frames = page.frames
        dlog(f"[DOM] frames={len(frames)} urls={[ (f.name, (f.url or '')[:60]) for f in frames]}")
        RX = re.compile(r"(다음|Next|결제|확인)")
        async def _scan(scp, tag):
            loc = scp.locator("button, a, [role='button'], input[type=button], input[type=submit]")
            cnt = await loc.count()
            hits = []
            for i in range(min(cnt, 30)):
                el = loc.nth(i)
                try:
                    txt = await el.inner_text()
                    if not RX.search(txt or ""): continue
                    vis = await el.is_visible()
                    en  = await el.is_enabled()
                    ar  = await el.get_attribute("aria-disabled")
                    dis = await el.get_attribute("disabled")
                    hits.append((i, txt.strip(), vis, en, ar, dis))
                except: pass
            if hits:
                dlog(f"[NEXT] {tag} candidates={hits}")
        await _scan(page, "page")
        for i, f in enumerate(frames):
            try:
                await _scan(f, f"frame#{i}")
            except: pass
    except Exception as e:
        dlog(f"[DOM] dump error: {e}")

async def dump_ticket_counters(scope) -> None:
    """우측 패널/히든필드의 수량이 왜 0인지 진단"""
    try:
        js = """
        () => {
          const q = (sel)=>document.querySelector(sel);
          const val=(el)=>el? (el.value ?? el.textContent ?? '') : '';
          const out = {};
          out.panel   = (document.body.innerText.match(/선택티켓\\s*정보\\s*\\((\\d+)매\\)/)||[])[1]||'';
          out.sel0    = val(q('select[name*=\"count\" i]')) || val(q('#sellCnt')) || '';
          out.hidden1 = val(q('[name=\"selectedTicketCount\"]')) || val(q('#selectedSeatCnt')) || '';
          out.hidden2 = val(q('[name=\"rsVolume\"]')) || '';
          out.seatId  = val(q('[name*=\"seatId\" i], #seatId')) || '';
          return out;
        }"""
        state = await (scope.evaluate(js) if hasattr(scope, "evaluate") else scope.page.evaluate(js))
        dlog(f"[DOM] counters {state}")
    except Exception as e:
        dlog(f"[DOM] counters error: {e}")

async def assert_visibility(page: Page):
    if not DEBUG_DOM: return
    try:
        vis = await page.evaluate("({v:document.visibilityState,f:document.hasFocus()})")
        dlog(f"[VIS] visibility={vis.get('v')} focus={vis.get('f')}")
    except: pass
async def process_one(ctx, sd: str) -> RunResult:
    page = await ctx.new_page()
    try:
        # 작품 페이지 → 예매창
        res_url = BASE_RESMAIN.format(sd=sd)
        await page.goto(res_url, wait_until="domcontentloaded", timeout=OPEN_TIMEOUT)
        title = (await find_title(page)) or f"sdCode {sd}"
        log(f"🎬 [{sd}] {title}")

        work = await open_booking_from_resmain(page)
        if work is None or work.is_closed():
            return RunResult(sd, title, False, "-", "예매창 열기 실패")
        await work.wait_for_load_state("domcontentloaded", timeout=STEP_TIMEOUT)
        # ▶ ADD: FCFS도 강제로 booking iFrame 확보
        scope0 = await ensure_booking_iframe(work)  # Page 또는 Frame가 될 수 있음
        scope0 = getattr(scope0, "page", None) or scope0
        booking_scope = scope0 or await find_booking_scope(work)
        setattr(work, "_booking_scope", booking_scope)
        # step_to_payment 등에서 쓰게 한 번 꽂아둡니다.
        try:
            work._scope0 = scope0
        except Exception:
            pass
        await attach_debuggers(work)
        await assert_visibility(work)

        cur = work.url or ""
        if "mypage/tickets/list" in cur or "biff.kr/kor/addon" in cur:
            return RunResult(sd, title, False, cur, "안내/마이페이지 리다이렉트(구매불가)")

        # 파라미터 모으기
        params = await wait_params_from_network(work, timeout_ms=5000)
        params.setdefault("chnlCd","WEB")
        params.setdefault("sdCode", sd)

        # ▶ ADD: iFrame/DOM에서 보강 수집
        dom_params = await harvest_params_from_dom(scope0 or work)
        params.update({k:v for k,v in dom_params.items() if v and not params.get(k)})
        # DOM 보강 바로 아래에 추가
        scope_onestop = await ensure_filmonestop_scope(scope0 or work)
        if not scope_onestop:
            # 예약 프레임/팝업을 실제로 띄워서 프레임 핸들 확보
            scope_onestop = await ensure_booking_iframe(work)
        if not scope_onestop:
            raise RuntimeError("no booking scope (iframe/popup) yet")

        pack = await ensure_full_rs_params(scope_onestop, params.get("prodSeq"), params.get("sdSeq"))
        params.update({k: v for k, v in pack.items() if v})

        # ★ 필수 값 가드 (여기서 없으면 바로 스킵/에러)
        need = ("prodSeq","sdSeq","perfDate")
        missing = [k for k in need if not params.get(k)]
        if missing:
            raise RuntimeError(f"missing params: {missing} (sdCode={sd})")

        # RS 예열 (csrfToken은 옵션, 내부에서 보강됨)
        await _prepare_session_like_har(
            scope_onestop,
            prodSeq=params["prodSeq"],
            sdSeq=params["sdSeq"],
            perfDate=params["perfDate"]
        )
        # ⭐ filmonestop 쿠키/오리진 정착 (RS 401/500 방지)
        await ensure_onestop_cookies(scope0 or work, params.get("prodSeq"), params.get("sdSeq"))
        # 🔁 회차 세션 스왑 (HAR 시퀀스 준수)
        await _swap_session_to_sdseq(
            scope0,
            params.get("prodSeq"), params.get("sdSeq"),
            chnlCd=params.get("chnlCd","WEB"),
            saleTycd=params.get("saleTycd","SALE_NORMAL"),
            saleCondNo=params.get("saleCondNo","1"),
            perfDate=params.get("perfDate",""),
            csrfToken=params.get("csrfToken","")
        )
        # iFrame(= scope0)에서 직접 좌석 요약 땡겨와서 로그
        total, remain, by, plan_type = await fetch_seat_summary(
            scope0,
            params.get("prodSeq"),
            params.get("sdSeq"),
            chnlCd=params.get("chnlCd","WEB"),
            saleTycd=params.get("saleTycd","SALE_NORMAL"),
            csrfToken=params.get("csrfToken")
        )
        ilog(f"[SEAT] sdCode={sd} sdSeq={params.get('sdSeq')} plan={plan_type} total={total} remain={remain} by={by}")

        meta = await fetch_basic_meta(
            scope0, params.get("prodSeq"), params.get("sdSeq"),
            chnlCd=params.get("chnlCd","WEB"),
            perfDate=params.get("perfDate",""),
            csrfToken=params.get("csrfToken",""),
            saleCondNo=params.get("saleCondNo","1"),
        )
        venue = meta.get("venue","")
        perf  = (params.get("perfDate") or "").strip()

        # 좌석맵 유무로 분기
        # 기존의 has_frame 기반 FCFS 오판 제거 → 실제 화면 단계로 판단
        scope = await find_booking_scope(work)
        if scope is None:
            log(f"🟡 [{sd}] filmonestop 예약 프레임/팝업이 아직 없음 → 버튼/팝업 재시도 단계")
        else:
            if await is_seat_page(scope):
                log(f"🔵 [{sd}] 좌석맵 감지 → 좌석 1개 선택 후 Next")
            elif await is_price_page(scope):
                log(f"🟡 [{sd}] 가격/티켓유형 단계 감지 → 수량 1 셋팅 후 Next")
            elif await is_zone_page(scope):
                log(f"🟡 [{sd}] 존 선택 단계 감지 → 임의 존 선택 후 Next")
            else:
                log(f"🟡 [{sd}] 선착순/기타 단계 감지 → 수량 1 셋팅 후 Next")

        ok = await step_to_payment(work, sd, params, False)

        final_url = work.url if not work.is_closed() else "-"
        if ok:
            try: setattr(page, "_hold_open", True)  # ★ 성공 시 창 유지 플래그
            except: pass
            log(f"✅ [{sd}] 결제창 진입: {final_url}")
            return RunResult(sd, title, True, final_url)
        else:
            log(f"❌ [{sd}] 결제창 진입 실패 (url={final_url})")
            return RunResult(sd, title, False, final_url, "결제단계 진입 실패")
    except Exception as e:
        return RunResult(sd, "", False, page.url if not page.is_closed() else "-", f"예외: {e}")
    finally:
        try:
            hold = bool(getattr(page, "_hold_open", False))
        except:
            hold = False
        if (not hold) and (not page.is_closed()):
            await page.close()
async def run_all_concurrent(sd_codes: List[str]) -> List[RunResult]:
    async with async_playwright() as pw:
        # ✅ 크롬으로 실행 (크롬 미설치면: `playwright install chrome`)
        browser = await pw.chromium.launch(
            channel="chrome",  # ← 크로미움 말고 '설치된 크롬' 사용
            headless=False,
            args=[
                "--disable-popup-blocking",
                "--start-maximized",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        ctx = await browser.new_context(
            locale="ko-KR",
            viewport=None,                    # 창 크기 그대로(최대화) 사용
            record_har_path="rs_trace.har",   # HAR 저장 파일명
            record_har_omit_content=False,    # 요청/응답 본문까지 남김
        )
        try:
            if not await ensure_login(ctx):
                await ctx.close(); await browser.close(); return []

            sem = asyncio.Semaphore(MAX_CONCURRENCY)
            results: List[RunResult] = []

            async def runner(code: str):
                async with sem:
                    r = await process_one(ctx, code)
                    results.append(r)

            await asyncio.gather(*(runner(sd) for sd in sd_codes))
            keep = (os.getenv("KEEP_OPEN_ON_SUCCESS", "1") == "1") and any(r.ok for r in results)
            if not keep:
                await ctx.close(); await browser.close()
            return results
        finally:
            try:
                if not (locals().get("keep", False)):
                    await ctx.close()
                    await browser.close()
            except:
                pass


def print_summary(results: List[RunResult]):
    ok = [r for r in results if r.ok]
    ng = [r for r in results if not r.ok]
    print("\n" + "─"*72)
    print("📊 결과 요약")
    for r in ok:
        meta = []
        if r.venue:   meta.append(r.venue)
        if r.perfDate: meta.append(f"{r.perfDate[:4]}-{r.perfDate[4:6]}-{r.perfDate[6:8]}")
        if r.total >= 0 or r.remain >= 0:
            meta.append(f"총={r.total if r.total>=0 else '?'} 잔여={r.remain if r.remain>=0 else '?'}")
        if r.plan:
            meta.append(f"모드={r.plan}")
        suffix = (" | " + " | ".join(meta)) if meta else ""
        print(f"  ✅ [{r.sd}] {r.title}{suffix}  →  {r.url}")
    for r in ng:
        why = f" ({r.reason})" if r.reason else ""
        meta = []
        if r.venue: meta.append(r.venue)
        if r.perfDate: meta.append(f"{r.perfDate[:4]}-{r.perfDate[4:6]}-{r.perfDate[6:8]}")
        if r.total >= 0 or r.remain >= 0:
            meta.append(f"총={r.total if r.total>=0 else '?'} 잔여={r.remain if r.remain>=0 else '?'}")
        if r.plan: meta.append(f"모드={r.plan}")
        suffix = (" | " + " | ".join(meta)) if meta else ""
        print(f"  ❌ [{r.sd}] {r.title or '(제목미상)'}{why}{suffix}  →  {r.url or '-'}")
    print("─"*72 + "\n")


def ask_retry(failed_codes: List[str]) -> bool:
    if not failed_codes: return False
    try:
        ans = input(f"♻️ 실패 {len(failed_codes)}건 재시도할까요? (y/N): ").strip().lower()
        return ans == "y"
    except: return False

async def main_once(codes: List[str]) -> List[RunResult]:
    results = await run_all_concurrent(codes)
    print_summary(results)
    return results

if __name__ == "__main__":
    results = asyncio.run(main_once(SD_CODES))
    while True:
        failed = [r.sd for r in results if not r.ok]
        if not failed or not ask_retry(failed): break
        results = asyncio.run(main_once(failed))

# === helper: NRS summary with strong binding =================================
async def rs_block_summary(scope, prodSeq: str, sdSeq: str, chnlCd: str, perfDate: str,
                          saleCond: str, csrfToken: str, H_RS: dict) -> tuple[int,int,dict]:
    """blockSummary2 → (tot, remain, by)
    - perfDate가 비면 절대 부르지 말 것!
    - 실패/0일 땐 /rs/prod(listSch) → prodSummary 순으로 보강"""
    blk = await post_api(
        scope, "/rs/blockSummary2",
        {"langCd": "ko", "csrfToken": csrfToken or "",
         "prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
         "chnlCd": chnlCd, "perfDate": perfDate or "",
         "saleCondNo": saleCond or "1"},
        extra_headers=H_RS
    )
    block = {}
    if isinstance(blk, dict):
        s = blk.get("summary")
        if isinstance(s, list) and s: block = s[0]
        elif isinstance(s, dict):     block = s
    try:
        avail = int(block.get("admissionAvailPersonCnt") or block.get("restSeatCnt") or 0)
        tot   = int(block.get("admissionTotalPersonCnt") or block.get("saleSeatCnt") or block.get("rendrSeatCnt") or 0)
    except Exception:
        avail, tot = 0, 0
    if tot <= 0 or tot < avail:
        tot = max(tot, avail)
    if tot == 0:
        try:
            js = await post_api(scope, "/rs/prod",
                {"langCd":"ko","csrfToken": csrfToken or "",
                 "prodSeq": str(prodSeq), "sdSeq":"", "chnlCd": chnlCd,
                 "saleTycd":"SALE_NORMAL","saleCondNo": saleCond or "1","perfDate": ""},
                extra_headers=H_RS)
            if isinstance(js, str):
                import json as _json
                try: js = _json.loads(js) if js.strip()[:1] in "[{" else {}
                except Exception: js = {}
            sch = js.get("listSch") or [] if isinstance(js, dict) else []
            for it in _iter_dicts(sch):
                if str(it.get("sdSeq") or it.get("sdNo") or "") == str(sdSeq):
                    avail = int(it.get("remainCnt") or it.get("seatRemainCnt") or avail)
                    guess = int(it.get("seatCnt") or it.get("seatTotalCnt") or 0)
                    tot = max(tot, guess, avail)
                    break
        except Exception:
            pass
    if tot == 0 and perfDate:
        try:
            ps = await post_api(scope, "/rs/prodSummary",
                {"langCd":"ko","csrfToken": csrfToken or "",
                 "prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
                 "chnlCd": chnlCd, "perfDate": perfDate or "",
                 "saleCondNo": saleCond or "1"},
                extra_headers=H_RS)
            if isinstance(ps, dict):
                s = ps.get("summary") or {}
                avail = max(avail, int(s.get("admissionAvailPersonCnt") or s.get("restSeatCnt") or 0))
                tot   = max(tot,   int(s.get("admissionTotalPersonCnt") or s.get("saleSeatCnt") or s.get("rendrSeatCnt") or 0))
        except Exception:
            pass
    return tot, avail, {"NRS": avail}

# === HAR 파서: 총좌석/잔여 강제 추출 ==========================================
def parse_har_seats(har_path="rs_trace.har", out_json="har_seats.json"):
    import json, re, pathlib
    p = pathlib.Path(har_path)
    if not p.exists():
        print(f"[HAR] not found: {p}"); return {}
    with p.open("r", encoding="utf-8") as f:
        har = json.load(f)
    entries = (har.get("log", {}) or {}).get("entries", []) or []
    out = {}
    def body_of(e):
        try:
            ct = (((e.get("response") or {}).get("content") or {}))
            if ct.get("text"):
                return json.loads(ct["text"])
        except: return None
        return None
    for e in entries:
        url = (e.get("request") or {}).get("url","")
        if not ("filmonestopapi.maketicket.co.kr" in url): continue
        js = body_of(e)
        if not isinstance(js, (dict, list)): continue

        # blockSummary2 → admissionTotal/Avail
        if "/rs/blockSummary2" in url and isinstance(js, dict):
            summ = js.get("summary")
            if isinstance(summ, list) and summ: summ = summ[0]
            if isinstance(summ, dict):
                sd = str(summ.get("sdSeq") or summ.get("sdseq") or "")
                tot = int(summ.get("admissionTotalPersonCnt") or summ.get("saleSeatCnt") or summ.get("rendrSeatCnt") or 0)
                rem = int(summ.get("admissionAvailPersonCnt") or summ.get("restSeatCnt") or 0)
                if sd:
                    o = out.setdefault(sd, {"total":0,"remain":0})
                    o["total"] = max(o["total"], tot)
                    o["remain"] = max(o["remain"], rem)

        # 좌석맵 존 상세 → 좌석 수 합산
        if re.search(r"/seat/GetRs(Z|)ZoneSeatMapInfo|/seat/GetRsSeat(StatusList|BaseMap)", url) and isinstance(js, (dict, list)):
            # 매우 유연한 합산
            def iter_dicts(x):
                if isinstance(x, dict):
                    yield x
                    for v in x.values(): yield from iter_dicts(v)
                elif isinstance(x, list):
                    for it in x: yield from iter_dicts(it)
            # sdSeq 추적
            sd = ""
            for d in iter_dicts(js):
                for k in ("sdSeq","sd_seq","sessionSdSeq","sdCode"):
                    v = d.get(k) if isinstance(d, dict) else None
                    if v and len(str(v))<=6: sd = str(v); break
            total = avail = 0
            # seat unit 카운트
            for d in iter_dicts(js):
                if not isinstance(d, dict): continue
                if any(k in d for k in ("seatId","seat_id","seatNo","x","y","col","row")):
                    total += 1
                    st = (str(d.get("useYn") or d.get("seatState") or d.get("seatStateCd") or d.get("status") or "")).upper()
                    if st in ("Y","AVAILABLE","OK","SS01000","SS02000","SS03000"): avail += 1
                    if d.get("disabled") is False: avail += 1
            if sd and total:
                o = out.setdefault(sd, {"total":0,"remain":0})
                o["total"] = max(o["total"], total)
                o["remain"] = max(o["remain"], avail)
    # 저장
    pathlib.Path(out_json).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[HAR] saved → {out_json}")
    return out

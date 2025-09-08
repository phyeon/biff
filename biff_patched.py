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
import httpx

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
AVAILABLE_CODES = {"SS01000", "SS02000", "SS03000", "AVAILABLE", "OK"}
BASE_RESMAIN = "https://biff.maketicket.co.kr/ko/resMain?sdCode={sd}"
LOGIN_URL    = "https://biff.maketicket.co.kr/ko/login"
KEEP_BROWSER_ON_HOLD = bool(int(os.getenv("KEEP_BROWSER_ON_HOLD", "1")))
PAYMENT_DETECTED = False  # 결s제 프레임 감지 여부(전역)
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

# 상단 옵션 근처 (ADD)
WARMUP_STRICT = os.getenv("WARMUP_STRICT", "1") == "1"

# === SNAPSHOT/예약 좌석종류 우선순위 ==========================================
# 예: "GENERAL,WHEELCHAIR,BNK" (대소문자 무관)
SEAT_PREF = [s.strip().upper() for s in os.getenv("SEAT_PREF", "GENERAL,WHEELCHAIR,BNK").split(",") if s.strip()]
# 우선순위에 해당하는 좌석이 없으면:
#   0 → 아무 좌석(판매가능)도 허용 안 함(즉시 포기)
#   1 → 우선순위 없으면 '아무 판매가능 좌석'도 허용
FORCE_SEAT_PREF = bool(int(os.getenv("FORCE_SEAT_PREF", "1")))

# 스냅샷 후 잔여>0 이면 자동으로 hold/Next까지 시도할지
SNAPSHOT_HOLD = bool(int(os.getenv("SNAPSHOT_HOLD", "0")))
# 스냅샷 JSON 저장
SNAPSHOT_SAVE = bool(int(os.getenv("SNAPSHOT_SAVE", "1")))
# sdCode 공급(ENV → 파일 → 상수 SD_CODES 순으로 사용)
AUTO_SNAPSHOT_ENV = os.getenv("AUTO_SNAPSHOT_SD_CODES", "")
SD_CODE_FILE = os.getenv("AUTO_SNAPSHOT_FILE", "sd_codes.txt")
# === CORS DEMO: 옵션 (코드-내 직접 기입 버전) ================================
# ✏️ 여기 리스트에 "실습 대상" 서버 주소를 그대로 적으세요.
#    - 도메인: "filmonestopapi.maketicket.co.kr"
#    - 포트 포함: "127.0.0.1:8000", "localhost:9000"
#    - 와일드카드: "*.yourlab.example"
# CORS_DEMO_MODE: off | allow | reflect | block | proxy
#   * proxy : 서버-사이드 프록시로 재요청 + 응답에 반사형 CORS 헤더 부여
# ❗ 반드시 네가 소유/통제하는 교육용/랩 서버만 넣을 것.
CORS_DEMO_MODE = "off"  # off | allow | reflect | block | cycle
CORS_TEST_DOMAINS = [
    "filmonestopapi.maketicket.co.kr",
    "filmonestop.maketicket.co.kr",
    # "filmonestop.maketicket.co.kr",
    # "biff.maketicket.co.kr",
    # "127.0.0.1:8000",
    # "localhost:9000",
    # "*.yourlab.example",  # 필요시 와일드카드
]
# allow 모드에서 허용할 오리진. '*' 사용 시에는 브라우저 정책상 credentials 허용이 불가함.
CORS_ALLOW_ORIGIN = "https://localhost"

# === ADD: CSRF helpers =======================================================
# === MakeTicket 상수 & CSRF/Header helpers ===================================
# (이미 같은 이름이 있으면 그대로 쓰고, 없으면 기본값 세팅)
try:
    FILM_ONESTOP_HOST
except NameError:
    FILM_ONESTOP_HOST = "https://filmonestop.maketicket.co.kr"

try:
    FILM_ONESTOP_API
except NameError:
    FILM_ONESTOP_API = "https://filmonestopapi.maketicket.co.kr"

def _rs_referer(prodSeq=None, sdSeq=None):
    prodSeq = "" if prodSeq is None else str(prodSeq)
    sdSeq   = "" if sdSeq   is None else str(sdSeq)
    # 이 사이트는 끝에 슬래시가 붙어도 동작해서 원문 스타일 유지
    return f"{FILM_ONESTOP_HOST}/ko/onestop/rs?prodSeq={prodSeq}&sdSeq={sdSeq}/"

async def fetch_csrf_token(page):
    """페이지/쿠키/스토리지에서 CSRF 토큰을 최대한 뽑아봄."""
    token = None
    # 1) DOM(meta/input/window 변수)
    try:
        token = await page.evaluate("""() => {
            const pick = (sel) => {
              const el = document.querySelector(sel);
              return el ? (el.content || el.value || el.getAttribute('content') || el.getAttribute('value')) : null;
            };
            return pick('meta[name="csrf-token"]') ||
                   pick('meta[name="_csrf"]') ||
                   pick('input[name="_csrf"]') ||
                   pick('input[name="csrfToken"]') ||
                   (window.__CSRF_TOKEN__ || null);
        }""")
    except: pass
    if token:
        return token

    # 2) 쿠키 (스프링/일반 관례 키 전부 시도)
    try:
        cookies = await page.context.cookies()
        for name in ["XSRF-TOKEN", "CSRF-TOKEN", "X-CSRF-TOKEN", "_csrf", "csrfToken"]:
            for c in cookies:
                if c.get("name") == name and c.get("value"):
                    return c["value"]
    except: pass

    # 3) 스토리지
    try:
        token = await page.evaluate("""() => {
            try {
                return localStorage.getItem('X-CSRF-TOKEN') ||
                       sessionStorage.getItem('X-CSRF-TOKEN') || null;
            } catch(e) { return null; }
        }""")
    except: pass
    return token

async def ensure_csrf_and_headers(page, prodSeq=None, sdSeq=None, csrfToken=None):
    """CSRF 토큰을 확보하고, API 호출용 헤더 딕셔너리를 만들어서 돌려줌."""
    if not csrfToken:
        csrfToken = await fetch_csrf_token(page)
    referer = _rs_referer(prodSeq, sdSeq)
    H_RS = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": FILM_ONESTOP_HOST,
        "Referer": referer,
        "X-Requested-With": "XMLHttpRequest",
    }
    if csrfToken:
        H_RS["X-CSRF-TOKEN"] = csrfToken
    return csrfToken, H_RS

def api_url(path: str) -> str:
    """상대 경로면 FILM_ONESTOP_API로 붙여줌."""
    if path.startswith("http"):
        return path
    return FILM_ONESTOP_API + path


def _rs_url(prodSeq, sdSeq):
    # 트레일링 슬래시 절대 금지 (토큰 발급이 안되는 케이스 방지)
    return f"{FILM_ONESTOP_HOST}/ko/onestop/rs?prodSeq={prodSeq}&sdSeq={sdSeq}"

def _seat_url(prodSeq, sdSeq):
    return f"{FILM_ONESTOP_HOST}/ko/onestop/rs/seat?prodSeq={prodSeq}&sdSeq={sdSeq}"

async def _csrf_from_cookies(ctx) -> str:
    import urllib.parse as _u
    for d in (FILM_ONESTOP_HOST, FILM_ONESTOP_API):
        try:
            for c in await ctx.cookies(d):
                if c.get("name") in ("XSRF-TOKEN","CSRF-TOKEN","X-CSRF-TOKEN","csrfToken"):
                    return _u.unquote(c.get("value",""))
        except:
            pass
    return ""

async def ensure_csrf_token_for(p, prodSeq: str, sdSeq: str) -> str:
    """
    RS 페이지 방문 → DOM meta/cookie 확인 → same-origin fetch(/rs/prod)로 응답헤더 x-csrf-token 확보
    """
    # 1) RS HTML 들어가 쿠키/세션 시드
    try:
        await p.goto(_rs_url(prodSeq, sdSeq), wait_until="domcontentloaded", timeout=15000)
    except Exception:
        pass

    # 2) DOM meta
    try:
        meta = await p.evaluate(
            "document.querySelector('meta[name=_csrf]')?.content "
            "|| document.querySelector('meta[name=csrf-token]')?.content || ''"
        )
        if meta:
            return meta
    except Exception:
        pass

    # 3) Cookie
    ck = await _csrf_from_cookies(p.context)
    if ck:
        return ck

    # 4) same-origin fetch로 응답 헤더에서 x-csrf-token 가져오기
    try:
        token = await p.evaluate("""
            async (api) => {
                const r = await fetch(api + '/rs/prod', {
                    method: 'POST',
                    headers: {'X-Requested-With': 'XMLHttpRequest'},
                    credentials: 'include'
                });
                return r.headers.get('x-csrf-token') || '';
            }
        """, FILM_ONESTOP_API)
        if token:
            return token
    except Exception:
        pass

    return ""



# --- (선택) 환경변수/CLI가 있다면 아래에서 덮어쓰도록 유지 (있으면 사용, 없으면 무시) ---
import os
_env_mode    = os.getenv("CORS_DEMO_MODE")
_env_domains = os.getenv("CORS_TEST_DOMAINS")
_env_allow   = os.getenv("CORS_ALLOW_ORIGIN")
if _env_mode:    CORS_DEMO_MODE = _env_mode.lower()
if _env_domains: CORS_TEST_DOMAINS = [s.strip() for s in _env_domains.split(",") if s.strip()]
if _env_allow:   CORS_ALLOW_ORIGIN = _env_allow

def _sale_ok(seat: dict) -> bool:
    # 판매 가능 & 미예약으로 간주하는 필드들(대소문자/스네이크·카멜 혼용 대응)
    toY = lambda v: str(v or "").strip().upper() == "Y"
    sale = toY(seat.get("sale_yn") or seat.get("saleYn"))
    rsv  = toY(seat.get("rsv_yn")  or seat.get("rsvYn"))
    # 가끔 reserveYn / soldYn / useYn 같은 거 섞여오면 추가
    if "reserveYn" in seat: rsv = rsv or toY(seat.get("reserveYn"))
    if "soldYn"    in seat: sale = sale and (not toY(seat.get("soldYn")))
    return sale and (not rsv)

def _seat_label_raw(seat: dict) -> tuple[str, str]:
    """좌석 하나에서 이름/코드 후보를 추출 (좌석등급/유형/클래스 명·코드 등)"""
    name = (
        seat.get("seat_type_nm") or seat.get("seatTypeNm") or
        seat.get("seat_class_nm") or seat.get("seatClassNm") or
        seat.get("seat_grade_nm") or seat.get("seatGradeNm") or
        seat.get("seatTypeName") or seat.get("classNm") or ""
    )
    code = (
        seat.get("seat_type_code") or seat.get("seatTypeCode") or
        seat.get("seat_class_seq") or seat.get("seatClassSeq") or
        seat.get("seatGradeCd") or seat.get("seatGradeCode") or ""
    )
    return str(name), str(code)

def _classify_seat_kind(name: str, code: str) -> str:
    """좌석종류 분류 → GENERAL / WHEELCHAIR / BNK / ETC"""
    lo = (name or "").lower()
    co = (code or "").lower()
    # 휠체어: '휠', 'wheel' 키워드
    if ("휠" in lo) or ("wheel" in lo) or ("wchair" in lo):
        return "WHEELCHAIR"
    # 부산은행(스폰서/지정군): '부산', 'bnk', 'bank'
    if ("부산" in lo) or ("bnk" in lo) or ("bank" in lo) or ("busan" in lo) or ("bnk" in co):
        return "BNK"
    # 일반: '일반', 'general', 'std'
    if ("일반" in lo) or ("general" in lo) or ("standard" in lo) or ("std" in lo):
        return "GENERAL"
    return "ETC"

def _collect_ticket_types(ticketType_resp: dict) -> list[dict]:
    """
    /api/v1/rs/tickettype 응답을 좌석/티켓 타입 리스트로 정규화
    필드명이 유동적이라 넓게 수용.
    """
    out = []
    d = ticketType_resp or {}
    items = (
        d.get("ticketTypeList") or d.get("tkttypList") or d.get("list") or
        d.get("result") or []
    )
    for t in items:
        nm  = t.get("tkttypNm") or t.get("ticketTypeNm") or t.get("name") or t.get("nm")
        seq = t.get("tkttypSeq") or t.get("ticketTypeSeq") or t.get("seq")
        scs = t.get("seatClassSeq") or t.get("seat_class_seq") or t.get("seatClass")
        price = t.get("price") or t.get("ticketPrice") or t.get("salePrice") or 0
        out.append({
            "name": nm, "tkttypSeq": seq, "seatClassSeq": scs, "price": price
        })
    return out

def _seat_type_tally(statusList_resp: dict) -> dict:
    from collections import Counter
    cnt = Counter()
    seats = _extract_list(statusList_resp) if isinstance(statusList_resp, (dict, list)) else []
    for s in seats:
        if not _sale_ok(s):
            continue
        nm, cd = _seat_label_raw(s)
        kind = _classify_seat_kind(nm, cd)
        cnt[kind] += 1
    return dict(cnt)

def _format_by_kind(by_kind: dict) -> str:
    keys = ["GENERAL", "WHEELCHAIR", "BNK", "ETC"]
    frag = []
    for k in keys:
        if by_kind.get(k) is not None:
            frag.append(f"{k}:{by_kind.get(k,0)}")
    return ", ".join(frag)
async def _load_sd_list_from_anywhere(defaults: list[str]) -> list[str]:
    if AUTO_SNAPSHOT_ENV.strip():
        return [s.strip() for s in AUTO_SNAPSHOT_ENV.split(",") if s.strip()]
    try:
        if os.path.exists(SD_CODE_FILE):
            with open(SD_CODE_FILE, "r", encoding="utf-8") as f:
                got = [ln.strip() for ln in f if ln.strip()]
                if got: return got
    except:
        pass
    return defaults

async def snapshot_sd(page, sdCode: str) -> dict:
    """
    하나의 sdCode에 대해:
      - filmapi로 prodSeq/sdSeq 발견
      - RS 워밍업
      - prodSummary / blockSummary2 / seatBaseMap / seatStatusList / tickettype 조회
      - plan 판정 + 총좌석/잔여 및 '좌석종류별 잔여' 집계
    """
    # map_sd_from_filmapi()는 (prodSeq, sdSeq) 튜플을 반환하므로
    # dict를 돌려주는 get_meta_from_filmapi()로 스위치
    sched = await get_meta_from_filmapi(page, sdCode)
    if not sched: 
        return {"sdCode": sdCode, "__error__": "filmapi sched 없음"}

    prodSeq = str(sched.get("prodSeq") or sched.get("prodseq") or "")
    sdSeq   = str(sched.get("sdSeq")   or sched.get("sdseq")   or "")
    title   = sched.get("perfMainNm") or sched.get("perfNm") or "-"
    hall    = sched.get("hallNm") or "-"
    venue   = sched.get("venueNm") or "-"
    date    = (sched.get("sdDate") or "").replace(".", "-")
    time_   = sched.get("sdTime") or ""

    refs = build_onestop_referers({}, prodSeq, sdSeq)

    # 워밍업
    try:
        await post_api(page, "/rs/prod", form={}, extra_headers={"Referer": refs["rs"]})
    except:
        pass

    async def _safe(call, *a, **kw):
        try: return await call(*a, **kw)
        except Exception as e: return {"__error__": str(e)}

    rs_prodSummary   = _safe(post_api, page, "/rs/prodSummary",   form={}, headers={"Referer": refs["rs"]})
    rs_blockSummary2 = _safe(post_api, page, "/rs/blockSummary2", form={}, headers={"Referer": refs["rs"]})
    seat_baseMap     = _safe(post_api, page, "/seat/GetRsSeatBaseMap",
                            form={"prodSeq": prodSeq, "sdSeq": sdSeq},
                            headers={"Referer": refs["seat"]})
    seat_statusList  = _safe(post_api, page, "/seat/GetRsSeatStatusList",
                            form={"prodSeq": prodSeq, "sdSeq": sdSeq},
                            headers={"Referer": refs["seat"]})
    rs_ticketType    = _safe(post_api, page, "/api/v1/rs/tickettype",     form={}, headers={"Referer": refs["rs"]})
    prodSummary, blockSummary2, baseMap, statusList, ticketType = await asyncio.gather(
        rs_prodSummary, rs_blockSummary2, seat_baseMap, seat_statusList, rs_ticketType
    )

    # plan 감지
    def _get_plan(*objs):
        for o in objs:
            if isinstance(o, dict):
                v = o.get("plan_type") or o.get("planType") or o.get("plan")
                if v: return str(v).upper()
        return None
    plan = _get_plan(prodSummary, baseMap, blockSummary2) or "SEAT"
    print(f"[DEBUG] plan={plan}")


    total = remain = 0
    by_kind = {}

    if plan == "SEAT":
        # zoneList 총/잔여
        zl = (baseMap or {}).get("zoneList") or (baseMap or {}).get("zonelist") or []
        def _toi(x): 
            try: return int(str(x).strip())
            except: return 0
        total  = sum(_toi(z.get("total_seat_cnt") or z.get("totalSeatCnt") or 0) for z in zl)
        remain = sum(_toi(z.get("rest_seat_cnt")  or z.get("restSeatCnt")  or 0) for z in zl)
        # 좌석종류별 잔여(판매가능 좌석만)
        by_kind = _seat_type_tally(statusList)
        # --- PATCH: 집계형 응답 보강 ---
        agg = _extract_list(statusList)
        if agg:
            # AVAILABLE_CODES는 상단에 이미 있음: {"SS01000","SS02000","SS03000","AVAILABLE","OK"}
            t2, r2, by = _count_status_items(agg, available=tuple(AVAILABLE_CODES))
            print(f"[DEBUG] RS/statuslist size={len(agg)} sample={agg[:1]}")
            print(f"[DEBUG] by={sorted(by.items())} total={t2} remain={r2}")
            if (total or 0) == 0 and t2:
                total = t2
            if r2 is not None:
                remain = r2

        # (fallback) 자유석/선착순 등으로 status가 빈 경우 → tickettype로 근사
        if (not agg) or (total in (None, 0)) or (remain is None):
            lst = _extract_list(ticketType)  # 위에서 이미 동시에 받아둔 raw JSON
            if lst:
                t_total = t_remain = 0
                for row in lst:
                    tot  = int(row.get("admissionTotalPersonCnt") or row.get("totalPersonCnt") or 0)
                    sold = int(row.get("admissionPersonCnt")      or row.get("saleCnt")         or 0)
                    rem  = int(row.get("admissionAvailPersonCnt") or row.get("remainSeatCnt")   or row.get("restSeatCnt") or 0)
                    if not tot and (rem or sold):
                        tot = rem + sold
                    t_total += tot; t_remain += rem
                if t_total:
                    total  = t_total if (total in (None, 0)) else total
                    remain = t_remain if (remain is None) else remain
                    print(f"[DEBUG] fallback(listTicketType) total={t_total} remain={t_remain}")
        # --- /PATCH ---

        # zoneList가 잔여 0으로만 오면 statusList로 보강
        if remain == 0 and by_kind:
            remain = sum(by_kind.values())
    else:
        # NRS/ALL: blockSummary2 집계
        sums = await seat_counts_via_blocksummary2(page, prodSeq, sdSeq)
        sums = _coerce_summary(sums) 
        total  = int(sums.get("total")  or 0)
        remain = int(sums.get("remain") or 0)
        # 종류 구분 없음 → NRS 전체를 GENERAL로 표시(편의)
        by_kind = {"GENERAL": remain}

    ttypes = _collect_ticket_types(ticketType)

    return {
        "sdCode": sdCode, "prodSeq": prodSeq, "sdSeq": sdSeq,
        "title": title, "venue": venue, "hall": hall, "date": date, "time": time_,
        "plan": plan, "total": total, "remain": remain,
        "bySeatType": by_kind,
        "ticketTypes": ttypes,
    }

def _log_snapshot_line(s: dict | tuple):
    if isinstance(s, tuple):
        # (sdCode, payload) 형태 허용
        _, s = s
    if not isinstance(s, dict):
        print("ℹ️  (skip) snapshot is not a dict:", type(s).__name__)
        return
    sd    = s.get("sdCode","")
    title = s.get("title","-")
    hall  = s.get("hall","-")
    dtxt  = f"{s.get('date','')} {s.get('time','')}".strip()
    plan  = s.get("plan","-")
    tot   = s.get("total","-")
    rem   = s.get("remain","-")
    kinds = _format_by_kind(s.get("bySeatType") or {})
    sold  = " [매진]" if (isinstance(rem, int) and rem == 0) else ""
    print(f"ℹ️  [{sd}] {title} | {hall} | {dtxt} | plan={plan} | 총={tot} 잔여={rem}{sold}" + (f" | 종류별: {kinds}" if kinds else ""))

async def snapshot_many(page, sd_list: list[str]) -> list[dict]:
    sem = asyncio.Semaphore(MAX_CONCURRENCY or 4)
    async def one(sd):
        async with sem:
            return await snapshot_sd(page, sd)
    return await asyncio.gather(*[one(sd) for sd in sd_list])

async def run_auto_snapshots(page, *, do_hold: bool | None = None):
    do_hold = SNAPSHOT_HOLD if do_hold is None else do_hold
    await install_cors_demo(page.context)  # CORS 교육용 라우팅

    sd_list = await _load_sd_list_from_anywhere(SD_CODES)
    snaps = await snapshot_many(page, sd_list)

    for s in snaps:
        _log_snapshot_line(s)

    if SNAPSHOT_SAVE:
        import json, time, pathlib
        ts = time.strftime("%Y%m%d-%H%M%S")
        out = pathlib.Path(f"./snapshots-{ts}.json")
        out.write_text(json.dumps(snaps, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"📝 snapshot saved: {out}")

    if do_hold:
        # 잔여>0 & 좌석종류 우선순위 맞춰 선점 시도
        for s in snaps:
            try:
                if int(s.get("remain") or 0) <= 0:
                    continue
                moved = await try_hold_one_with_pref(page, s, SEAT_PREF)
                if moved:
                    print(f"✅ [{s.get('sdCode')}] 결제단계 진입 시도 완료")
                    await arm_payment_hold(page)  # 네 기존 로직
            except Exception as e:
                print(f"[{s.get('sdCode')}] hold 실패: {e}")
def _collect_candidates_by_pref(statusList_resp: dict, prefs: list[str]) -> list[dict]:
    """
    좌석 상태에서 '판매가능'인 좌석들 중 prefs 우선순위대로 정렬해 반환
    각 원소는 {"seatId": ..., "kind": "..."} 최소 키 포함
    """
    seats = (statusList_resp or {}).get("seatList") or []
    buckets = {k: [] for k in ["GENERAL","WHEELCHAIR","BNK","ETC"]}
    for s in seats:
        if not _sale_ok(s):
            continue
        nm, cd = _seat_label_raw(s)
        kind = _classify_seat_kind(nm, cd)
        seat_id = (s.get("seat_id") or s.get("seatId") or s.get("seat_id_seq") or s.get("id"))
        if seat_id:
            buckets.setdefault(kind, []).append({"seatId": seat_id, "kind": kind, "raw": s})
    # 우선순위대로 합치기
    ordered = []
    used = set()
    for k in (prefs or []):
        for item in buckets.get(k, []):
            sid = item["seatId"]
            if sid in used: continue
            used.add(sid); ordered.append(item)
    if FORCE_SEAT_PREF and ordered:
        return ordered
    # 우선순위 좌석이 하나도 없으면 '그 외'까지
    for k in ["GENERAL","WHEELCHAIR","BNK","ETC"]:
        for item in buckets.get(k, []):
            sid = item["seatId"]
            if sid in used: continue
            used.add(sid); ordered.append(item)
    return ordered

async def try_hold_one_with_pref(scope, snap: dict, prefs: list[str]) -> bool:
    """
    snap(=snapshot_sd 결과) 기반으로 좌석 1매 선점/Next 시도
    - plan=SEAT: statusList에서 prefs 우선순위 좌석 pick → pick_seat_via_api(...)
    - plan=NRS/ALL: 수량=1 Next
    """
    prodSeq = snap.get("prodSeq") or ""
    sdSeq   = snap.get("sdSeq")   or ""
    if not (prodSeq and sdSeq):
        return False

    # RS 필수 파라미터 확보
    pack = await ensure_full_rs_params(scope, prodSeq, sdSeq)
    refs = build_onestop_referers(scope, str(prodSeq), str(sdSeq))

    plan = (snap.get("plan") or "").upper()
    remain = int(snap.get("remain") or 0)

    if remain <= 0:
        return False

    if plan == "SEAT":
        # 최신 statusList를 다시 긁어 후보 좌석 선정
        statusList = await post_api(scope, "/seat/GetRsSeatStatusList",
                                    form={"prodSeq": prodSeq, "sdSeq": sdSeq},
                                    extra_headers={"Referer": refs["seat"]})
        cand = _collect_candidates_by_pref(statusList, [p.upper() for p in (prefs or [])])
        if not cand:
            return False

        # 네 기존 함수가 seat_ids 인자를 받도록 래핑 (없으면 qty=1로 동작)
        seat_ids = [c["seatId"] for c in cand[:10]]
        picked = False
        try:
            # (이미 구현되어 있던 버전이 있을 때)
            picked = await pick_seat_via_api(scope, prodSeq, sdSeq, qty=1, seat_ids=seat_ids)
        except TypeError:
            # 구버전이면 그냥 qty=1만 보내고, 내부에서 랜덤/첫 좌석 선택
            picked = await pick_seat_via_api(scope, prodSeq, sdSeq, qty=1)
        if not picked:
            return False
        return await ensure_qty_one_and_next(scope, referer=refs["seat"])

    # NRS/ALL
    return await ensure_qty_one_and_next(scope, referer=refs["rs"])

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

async def ensure_booking_iframe(p, *, prodSeq: str | None = None, sdSeq: str | None = None):
    # 이미 열려 있나 먼저 확인
    sc = await find_booking_scope(p)
    if sc:
        return sc

    # 예매 유도 클릭들
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

    # iframe 유도 실패 시 filmonestop 직접 오픈 (prodSeq/sdSeq가 있으면)
    if prodSeq and sdSeq:
        ctx = p.context
        tmp = await ctx.new_page()
        keep = False
        try:
            await tmp.goto(
                f"{FILM_ONESTOP_HOST}/ko/onestop/rs?prodSeq={prodSeq}&sdSeq={sdSeq}",
                wait_until="domcontentloaded", timeout=15000
            )
            sc = await ensure_filmonestop_scope(tmp)
            if sc:
                keep = True
                return sc
        finally:
            if not keep:
                await tmp.close()

    return await find_booking_scope(p)

# ==== PATCH: Referer 정규화 + CSRF 확보 유틸 =================================
def _clean_ref(url: str) -> str:
    return (url or "").rstrip("/")

def build_onestop_referers(page_or_scope, prodSeq: str, sdSeq: str):
    rs   = _clean_ref(_rs_url(prodSeq, sdSeq))
    seat = _clean_ref(_seat_url(prodSeq, sdSeq))
    return {"rs": rs, "seat": seat}

# ==== PATCH: 읽기성 API는 무CSRF 허용(선택) ===================================
ALLOW_NO_CSRF = os.getenv("ALLOW_NO_CSRF", "0") == "1"
READONLY_ENDPOINTS = {
    "/rs/prodSummary", "/rs/blockSummary2",
    "/seat/GetRsSeatBaseMap", "/api/v1/rs/tickettype",
    "/seat/GetRsSeatStatusList"
}

# post_api 내부에서 쓸 수 있게 파라미터 추가(없으면 무시)
# post_api(..., allow_no_csrf=True) 형태로도 강제 허용 가능하게


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

    title, venue, d8, t4 = "", "", "", ""
    try:
        ps = await post_api(scope0, "/rs/prodSummary", {
            "langCd":"ko","csrfToken": csrfToken or "",
            "prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
            "chnlCd": chnlCd, "perfDate": perfDate or "",
            "saleCondNo": saleCondNo or "1",
        }, extra_headers=H_RS)

        summ = ps.get("summary") if isinstance(ps, dict) else {}
        if isinstance(summ, list) and summ: 
            summ = summ[0]

        def pick(d, *keys):
            for k in keys:
                v = (d.get(k) if isinstance(d, dict) else None) or ""
                if v: 
                    return str(v).strip()
            return ""

        # 제목/장소/일시 후보키 폭넓게
        title = pick(summ, "perfMainNm","perfNm","movieNm","programNm","prodNm","title")
        venue = pick(summ, "operHallNm","hallNm","placeNm","siteName","screenNm","venueNm")

        d8 = pick(summ, "sdStartDt","sdStartDay","perfStartDay","perfDate","sdDate")
        t4 = pick(summ, "sdStartTm","perfStartTm","sdTime","startTime")

        # 정규화
        d8 = re.sub(r"[^0-9]","", d8)[:8]
        t4 = re.sub(r"[^0-9]","", t4)[:4]
    except Exception:
        pass

    return {"title": title, "venue": venue, "perfDate": d8, "sdTime": t4}

# --- get_meta_from_filmapi (REPLACE) ---
@trace_step("filmapi_meta")
async def get_meta_from_filmapi(scope_or_page, sdCode: str) -> dict:
    sd = str(sdCode).zfill(3)
    url = f"https://filmapi.maketicket.co.kr/api/v1/prodList?sdCode={sd}"
    js = await fetch_json(scope_or_page, url)
    items = js if isinstance(js, list) else (js.get("prodList") or js.get("data") or js.get("list") or [])
    if not isinstance(items, list) or not items:
        return {}
    row = next((x for x in items if str(x.get("sdCode")).zfill(3) == sd), items[0])

    import re as _re
    digits = lambda s: _re.sub(r"[^0-9]", "", str(s or ""))

    return {
        "title": row.get("perfMainNm") or row.get("perfNm") or row.get("movieNm") or row.get("programNm") or row.get("prodNm"),
        "venue": row.get("venueNm") or "",
        "hall":  row.get("hallNm") or "",
        "sdDate": digits(row.get("sdDate")),
        "sdTime": digits(row.get("sdTime")),
        "prodSeq": str(row.get("prodSeq") or ""),
        "sdSeq":   str(row.get("sdSeq") or ""),
        "remainSeat": row.get("remainSeat"),
    }

# --- map_sd_from_filmapi (REPLACE) ---
@trace_step("map_sd_from_filmapi")
async def map_sd_from_filmapi(scope_or_page, sdCode: str) -> tuple[str, str]:
    sd = str(sdCode).zfill(3)
    url = f"https://filmapi.maketicket.co.kr/api/v1/prodList?sdCode={sd}"
    js = await fetch_json(scope_or_page, url)
    items = js if isinstance(js, list) else (js.get("prodList") or js.get("data") or js.get("list") or [])
    if isinstance(items, list):
        for x in items:
            if str(x.get("sdCode")).zfill(3) == sd:
                return str(x.get("prodSeq") or ""), str(x.get("sdSeq") or "")
    return "", ""

# === CORS DEMO 라우팅 설치 ====================================
# ⚠️ 교육/실습 목적: 네가 명시한 도메인(= CORS_TEST_DOMAINS)에만 적용됨.
#    제3자 서비스에 임의로 적용하지 마. (테스트 서버/네 소유 도메인 권장)
import asyncio
from typing import List

async def install_cors_demo(ctx):
    """
    Playwright BrowserContext에 CORS 데모 라우팅을 설치한다.
    - off: 아무것도 하지 않음
    - allow: 응답에 ACAO/ACAM/ACAH 추가
    - reflect: 요청 'Origin'을 그대로 반사(ACAO=Origin), Vary: Origin
    - block: 모든 'Access-Control-Allow-*' 헤더 제거 (엄격 모드 시뮬)
    """
    mode = (CORS_DEMO_MODE or "off").lower()
    if mode == "off" or not CORS_TEST_DOMAINS:
        return

    # 입력이 'api.example.com' 같은 도메인이면 '**://host/*' 패턴으로 바꿔줌
    patterns: List[str] = []
    for d in CORS_TEST_DOMAINS:
        if "://" in d or d.startswith(("**://", "*://")):
            patterns.append(d)
        else:
            patterns.append(f"**://{d}/*")

    async def _handler(route):
        req = route.request

        # --- 1) 프리플라이트(OPTIONS) 우선 처리 ---
        if req.method.upper() == "OPTIONS":
            allow_origin = ""
            if mode in ("allow","reflect","proxy"):
                allow_origin = (
                    CORS_ALLOW_ORIGIN if mode == "allow"
                    else (req.headers.get("origin") or "")
                )
            if allow_origin:
                hdrs = {
                    "access-control-allow-origin": allow_origin,
                    "access-control-allow-methods": req.headers.get("access-control-request-method", "GET,POST,PUT,PATCH,DELETE,OPTIONS"),
                    "access-control-allow-headers": req.headers.get("access-control-request-headers", "*"),
                }
                if allow_origin != "*":
                    hdrs["access-control-allow-credentials"] = "true"
                return await route.fulfill(status=204, headers=hdrs, body="")
            else:
                return await route.fulfill(status=403, body="")

        # --- 2) 실요청: 원래 응답을 받아서 헤더만 조정 ---
        mode_l = mode  # 가독성
        if mode_l == "proxy":
            # 2-1. 서버-사이드로 재요청 (CORS 영향 없음)
            banned = {"origin","referer","host","content-length"}
            fwd_headers = {k:v for k,v in req.headers.items() if k.lower() not in banned}
            # 요청 바디 추출
            pd_bytes = req.post_data_buffer
            if pd_bytes is None:
                pd = req.post_data  # 속성! (메서드 아님)
                pd_bytes = pd.encode("utf-8") if isinstance(pd, str) and pd else None
            body_bytes = pd_bytes
            async with httpx.AsyncClient(follow_redirects=True, timeout=20.0) as client:
                r = await client.request(
                    req.method, req.url,
                    headers=fwd_headers,
                    content=body_bytes
                )
                # 2-2. 응답 헤더 정리(+CORS 반사)
                headers = dict(r.headers)
                # 민감/충돌 헤더 제외
                drop_prefix = ("content-encoding","transfer-encoding","content-length","connection")
                headers = {k:v for k,v in headers.items() if k.lower() not in drop_prefix}
                # CORS: 요청 Origin을 그대로 반사(+ credentials), 데모용
                origin = req.headers.get("origin") or ""
                if origin:
                    headers["Access-Control-Allow-Origin"] = origin
                    headers["Vary"] = "Origin"
                    headers["Access-Control-Allow-Credentials"] = "true"
                    headers.setdefault("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
                    headers.setdefault("Access-Control-Allow-Headers", "*")

                # 2-3. fulfill
                return await route.fulfill(
                    status=r.status_code,
                    body=r.content,
                    headers=headers
                )

        # (기존 분기) allow/reflect/block은 원래대로
        upstream = await route.fetch()
        headers = dict(upstream.headers)
        lower   = {k.lower(): v for k, v in headers.items()}
        if mode == "allow":
            ao = CORS_ALLOW_ORIGIN
            headers["Access-Control-Allow-Origin"] = ao
            if ao != "*":
                headers.setdefault("Access-Control-Allow-Credentials", "true")
            headers.setdefault("Access-Control-Allow-Methods","GET,POST,PUT,PATCH,DELETE,OPTIONS")
            headers.setdefault("Access-Control-Allow-Headers","*")
        elif mode == "reflect":
            origin = req.headers.get("origin") or ""
            if origin:
                headers["Access-Control-Allow-Origin"] = origin
                headers["Vary"] = "Origin"
                headers["Access-Control-Allow-Credentials"] = "true"
        elif mode == "block":
            # 모든 'access-control-allow-*' 헤더 제거
            headers = {k: v for k, v in headers.items() if not k.lower().startswith("access-control-allow-")}

        return await route.fulfill(response=upstream, headers=headers)

    # 패턴마다 라우트 설치
    for pat in patterns:
        await ctx.route(pat, lambda r: asyncio.create_task(_handler(r)))

    # 로그(원하면 TR/ilog 같은 네 로깅 유틸로 바꿔도 됨)
    print(f"[CORS-DEMO] mode={mode} targets={patterns} allow_origin={CORS_ALLOW_ORIGIN}")

# --- PATCH: helpers (put under imports) ---
def _extract_list(payload):
    """응답 JSON 어디에 list가 있든 꺼내줌."""
    if isinstance(payload, dict):
        for k in ("rsSeatStatusList", "seatStatusList", "list", "rows", "resultList"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
        for k in ("data", "result", "payload"):
            v = payload.get(k)
            if isinstance(v, dict) and isinstance(v.get("list"), list):
                return v["list"]
    return []

def _count_status_items(items, available=("SS01000", "AVAILABLE", "OK")):
    """집계: 총=모든 status 합, 잔여=available 코드 합."""
    total = 0
    by = {}
    for it in items or []:
        if not isinstance(it, dict):
            continue
        cd = str(it.get("seatStatusCd") or it.get("statusCd") or it.get("cd") or "").upper()
        cnt = int(it.get("seatCnt") or it.get("cnt") or it.get("count") or 0)
        if not cd or cnt <= 0:
            continue
        total += cnt
        by[cd] = by.get(cd, 0) + cnt
    remain = sum(by.get(k, 0) for k in available)
    return total, remain, by
# --- /PATCH ---



# --- helper (ADD) ---
def _sum_from_basemap(js: dict) -> tuple[int,int,dict]:
    total = remain = 0
    zones = []
    if isinstance(js, dict):
        for k in ("zoneList","zones","data","list","items"):
            v = js.get(k)
            if isinstance(v, list):
                zones = v; break
    by = {}
    for z in (zones or []):
        if not isinstance(z, dict): continue
        name = (z.get("zoneNm") or z.get("zone_name") or z.get("name") or "").strip()
        t = int(z.get("total_seat_cnt") or z.get("totalSeatCnt") or z.get("seatCnt") or 0)
        r = int(z.get("rest_seat_cnt")  or z.get("remainSeatCnt") or z.get("availableSeatCnt") or 0)
        total += max(t,0); remain += max(r,0)
        if name: by[name] = r
    return total, remain, by


@trace_step("plan_type")
async def get_plan_type(scope0, prodSeq: str, sdSeq: str, perfDate: str="", csrfToken: str="") -> str:
    # filmonestop 스코프 & seat 참조헤더
    scope0 = await ensure_scope_or_spawn(scope0, str(prodSeq), str(sdSeq)) or scope0
    REFS   = build_onestop_referers(scope0, str(prodSeq), str(sdSeq))
    H_SEAT = _ref_headers(REFS, "seat")
    try:
        js = await post_api(
            scope0, "/seat/GetRsSeatBaseMap",
            {"prod_seq": str(prodSeq), "sd_seq": str(sdSeq), "chnl_cd": "WEB", "sale_tycd": "SALE_NORMAL"},
            extra_headers=H_SEAT
        )
        pt = (js or {}).get("plan_type") or (js or {}).get("planType") or ""
        pt = str(pt).upper()
        if pt in ("SEAT","ZONE","RS"): return "SEAT"   # RS는 지정석
        if pt in ("NRS","FREE"):       return "NRS"
        return pt or "ALL"
    except Exception as e:
        dlog(f"[plan_type] fallback: {e}")
        return "ALL"


@trace_step("collect_show_info")
async def collect_show_info(scope0, prodSeq: str, sdSeq: str, *, sdCode: str="") -> dict:
    # 0) sdCode가 왔고 seq가 비어 있으면 filmapi로 역매핑 먼저
    if (not prodSeq or not sdSeq) and sdCode:
        try:
            p2, s2 = await map_sd_from_filmapi(scope0, sdCode)
            if p2 and s2:
                prodSeq, sdSeq = p2, s2
        except Exception as e:
            dlog(f"[collect_show_info] sdCode map fail: {e}")

    # 1) RS 필수 파라미터 확보 (이제 seq가 있으므로 스폰 성공)
    pack = {}
    try:
        pack = await ensure_full_rs_params(scope0, prodSeq, sdSeq)
    except Exception as e:
        dlog(f"[collect_show_info] ensure_full_rs_params fail: {e}")
    perfDate  = (pack or {}).get("perfDate","")
    csrfToken = (pack or {}).get("csrfToken","")

    # 2) 기본 메타(기존 경로)
    title_first = (await find_title(scope0)) or ""
    title = title_first
    meta  = {}
    try:
        meta = await fetch_basic_meta(scope0, prodSeq, sdSeq, perfDate=perfDate, csrfToken=csrfToken)
    except Exception as e:
        dlog(f"[collect_show_info] fetch_basic_meta fail: {e}")

    venue = (meta or {}).get("venue","") or ""
    if (not title) or (title.strip() == "공연티켓 - 예매"):
        t = (meta or {}).get("title","")
        if t: title = t
    if not perfDate:
        perfDate = (meta or {}).get("perfDate","") or perfDate
    sdTime = (meta or {}).get("sdTime","") or locals().get("sdTime","")

    # 3) filmapi 보강(sdCode 있으면)
    f = {}
    if sdCode:
        f = await get_meta_from_filmapi(scope0, sdCode)

    film_title = (f.get("title","") or "").strip()
    if film_title:
        title = film_title
    # venue/hall 조합이 하나라도 있으면 최우선
    v = (f.get("venue","") or "").strip()
    h = (f.get("hall","")  or "").strip()
    if (v or h) and not venue:
        venue = f"{v} {h}".strip()

    # 여전히 비면 플레이스홀더
    if not title or title.strip() == "공연티켓 - 예매":
        title = title_first or title or "제목확인필요"
    if not venue:
        # prodSummary 키 확장 폴백(파일에 없다면 추가)
        venue = pick(meta, "operHallNm","hallNm","placeNm","siteName","screenNm","venueNm",
                        "perfPlaceNm","theaterNm","cinemaNm","operPlaceNm","operSiteNm") or "장소확인필요"

    # 제목/장소 우선순위: filmapi > 기존
    film_title = (f.get("title","") or "").strip()
    if film_title:
        title = film_title

    v = (f.get("venue","") or "").strip()
    h = (f.get("hall","")  or "").strip()
    if v or h:
        venue = f"{v} {h}".strip()

    # 날짜/시간 보강
    if not perfDate and f.get("sdDate"):
        perfDate = f.get("sdDate")
    sdTime = f.get("sdTime","") or locals().get("sdTime","")

    # 4) 좌석 요약
    total = remain = 0
    plan = ""
    by = {}
    try:
        total, remain, by, plan = await fetch_seat_summary(
            scope0, prodSeq, sdSeq,
            csrfToken=csrfToken, perfDate=perfDate
        )
    except Exception as e:
        dlog(f"[SEAT] summary fail: {e}")
        # filmapi 잔여라도 사용(있으면)
        if sdCode:
            try:
                f2 = f or await get_meta_from_filmapi(scope0, sdCode)
            except Exception:
                f2 = {}
            if isinstance(f2, dict) and f2.get("remainSeat") is not None:
                remain = int(f2["remainSeat"])
                plan = plan or "NRS"
        plan = plan or "ALL"

    # 5) plan 재보정 (ALL이면 좌석맵으로 확인)
    if (not plan) or (plan == "ALL"):
        plan = await get_plan_type(scope0, prodSeq, sdSeq, perfDate=perfDate, csrfToken=csrfToken)

    # --- PATCH: force seat plan ---
    plan = "SEAT"
    print(f"[DEBUG] plan={plan}")
    # --- /PATCH ---

    # 6) 자유석인데 잔여=0으로 나오면 filmapi 잔여로 보강
    if plan == "NRS" and remain == 0 and f.get("remainSeat",0) > 0:
        remain = int(f["remainSeat"])

    return {
        "title": title.strip() or "제목확인필요",
        "venue": venue.strip() or "장소확인필요",
        "perfDate": str(perfDate or ""),
        "sdTime": sdTime if 'sdTime' in locals() else "",
        "total": int(total or 0),
        "remain": int(remain or 0),
        "plan": plan or "ALL",
    }

def _fmt_date(d8: str) -> str:
    return f"{d8[:4]}-{d8[4:6]}-{d8[6:]}" if d8 and len(d8)>=8 else d8

def _fmt_time(t4: str) -> str:
    return f"{t4[:2]}:{t4[2:]}" if t4 and len(t4)>=4 else t4

def log_show_line(sdCode: str, info: dict):
    ts = _fmt_date(info.get("perfDate",""))
    tt = _fmt_time(info.get("sdTime",""))
    title = (info.get("title","") or "").strip()
    venue = (info.get("venue","") or "").strip()
    plan  = (info.get("plan","") or "").strip().upper()
    total = info.get("total", 0)
    remain= info.get("remain", 0)

    tot_s = str(total) if (total is not None) else "-"

    when  = f"{ts} {tt}".strip()
    sold = (info.get("soldOut") is True) or (remain == 0)
    flag = " [매진]" if sold else ""
    log(f"ℹ️  [{sdCode}] {(title or '제목확인필요') + flag} | {venue or '장소확인필요'} | {when} | plan={plan or 'ALL'} | 총={tot_s} 잔여={remain}")


# [REPLACE] DOM/전역/URL에서 기본 파라미터 캐치 (sdCode 포함)
async def _pick_from_dom_or_global(scope):
    js = r"""
    () => {
      const pick = (...sels) => {
        for (const s of sels) {
          const el = document.querySelector(s);
          if (el) return (el.value || el.getAttribute('content') || el.textContent || '').trim();
        }
        return '';
      };
      const qs = new URL(location.href).searchParams;
      const out = {
        prodSeq:  pick('#prodSeq','[name="prodSeq"]','[name="prod_seq"]') || qs.get('prodSeq') || '',
        sdSeq:    pick('#sdSeq','[name="sdSeq"]','[name="sd_seq"]')       || qs.get('sdSeq')   || '',
        perfDate: (pick('#perfDate','[name="perfDate"]') || qs.get('perfDate') || '').replace(/-/g,''),
        csrfToken:pick('#csrfToken','meta[name="csrf-token"]') || (window._csrf || ''),
        sdCode:   pick('#sdCode','[name="sdCode"]') || qs.get('sdCode') || ''
      };
      if (!out.prodSeq || !out.sdSeq) {
        const html = document.documentElement.innerHTML;
        const m1 = html.match(/prodSeq["']?\s*[:=]\s*["']?(\d+)/i); if (m1) out.prodSeq = m1[1];
        const m2 = html.match(/sdSeq["']?\s*[:=]\s*["']?(\d+)/i);   if (m2) out.sdSeq   = m2[1];
      }
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

# --- REPLACE: ensure_full_rs_params (robust spawn + harvest) ---
async def ensure_full_rs_params(scope, prodSeq: str|None, sdSeq: str|None) -> dict:
    """
    prodSeq/sdSeq를 넘기면 나머지(perfDate/csrfToken)를 자동 보강.
    DOM/글로벌/URL → filmapi(sdCode) → HAR → 네트워크 하베스트 → /rs/prod 역조회 → 쿠키 순.
    """
    out = {"prodSeq": str(prodSeq or ""), "sdSeq": str(sdSeq or ""), "perfDate": "", "csrfToken": ""}

    # 1) DOM/글로벌/URL 1차
    prim = await _pick_from_dom_or_global(scope)
    out["prodSeq"]   = out["prodSeq"]   or (prim.get("prodSeq") or "")
    out["sdSeq"]     = out["sdSeq"]     or (prim.get("sdSeq") or "")
    out["perfDate"]  = (prim.get("perfDate") or "").replace("-", "") or ""
    out["csrfToken"] = prim.get("csrfToken") or out.get("csrfToken") or ""

    # 1.1) DOM에 seq 없고 sdCode만 있을 때 filmapi로 보강
    if (not out["prodSeq"] or not out["sdSeq"]):
        sd_code = (prim.get("sdCode") or "").strip() if isinstance(prim, dict) else ""
        if sd_code:
            p2, s2 = await map_sd_from_filmapi(scope, sd_code)
            if p2 and s2:
                out["prodSeq"] = str(p2)
                out["sdSeq"]   = str(s2)

    # 1.2) HAR에서 빠진 값 보강 (우선순위: sdCode→sdSeq→prodSeq)
    missing = [k for k in ("prodSeq","sdSeq","perfDate","csrfToken") if not out.get(k)]
    if missing:
        key = (prim.get("sdCode") or out.get("sdSeq") or out.get("prodSeq") or "").strip()
        if key:
            har = har_params_for(key)
            for k in ("prodSeq","sdSeq","perfDate","csrfToken"):
                if (not out.get(k)) and har.get(k):
                    out[k] = str(har[k])

    # 1.4) seq 채운 뒤 스폰 (여기서 스코프 고정)
    if not scope:
        sc = await ensure_scope_or_spawn(scope, str(out["prodSeq"]), str(out["sdSeq"]))
        scope = sc or scope
    if not scope:
        raise RuntimeError("ensure_full_rs_params: scope is None (after spawn)")

    # 1.5) 네트워크 요청에서 부족분 하베스트
    if not all(out.get(k) for k in ("prodSeq","sdSeq","perfDate","csrfToken")):
        try:
            pg = _as_page(scope)
            got = await wait_params_from_network(pg, timeout_ms=5000)
            for k in ("prodSeq","sdSeq","perfDate","csrfToken"):
                if got.get(k) and not out.get(k):
                    out[k] = got[k]
        except Exception as e:
            dlog(f"[COLLECT] net-harvest warn: {e}")

    # 2) perfDate 역조회 (/rs/prod listSch)
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

    # 3) csrf 쿠키 보강
    if not out["csrfToken"]:
        out["csrfToken"] = await _get_cookie(scope, "XSRF-TOKEN") or await _get_cookie(scope, "CSRF-TOKEN") or ""

    # 4) normalize
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

# === ADD: tiny dict helper ===
def pick(obj, *keys):
    """
    첫 번째로 값이 존재하는 키를 선택해서 문자열로 돌려준다.
    obj가 dict가 아니면 빈 문자열.
    """
    if not isinstance(obj, dict):
        return ""
    for k in keys:
        if k in obj:
            v = obj[k]
            if v is None:
                continue
            if isinstance(v, str):
                s = v.strip()
                if s:
                    return s
            else:
                return str(v)
    return ""


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
# === HAR RECORDER (auto .har) ======================================
import atexit, pathlib, time, json, urllib.parse

HAR_ENABLE = int(os.getenv("HAR_ENABLE", "1"))
HAR_DIR    = os.getenv("HAR_DIR", "./_har")

class _HarRec:
    def __init__(self):
        self.reset()
        try:
            pathlib.Path(HAR_DIR).mkdir(parents=True, exist_ok=True)
        except: pass

    def reset(self):
        ts = time.strftime("%Y%m%d-%H%M%S")
        self.started = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        self.path = str(pathlib.Path(HAR_DIR) / f"capture_{ts}.har")
        self.seq2idx = {}
        self.log = {
            "log": {
                "version": "1.2",
                "creator": {"name": "biff_har", "version": "1.0"},
                "entries": []
            }
        }

    def _headers_list(self, d):
        return [{"name": k, "value": str(v)} for k, v in (d or {}).items()]

    def on_req(self, seq, url, method, headers, body_text):
        if not HAR_ENABLE or not url: return
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query or "")
        entry = {
            "startedDateTime": self.started,
            "time": 0,
            "request": {
                "method": method,
                "url": url,
                "httpVersion": "HTTP/1.1",
                "headers": self._headers_list(headers),
                "queryString": [{"name": k, "value": (v[0] if isinstance(v,list) and v else "")} for k,v in qs.items()],
                "headersSize": -1,
                "bodySize": len(body_text or ""),
            },
            "response": {
                "status": 0, "statusText": "",
                "httpVersion": "HTTP/1.1",
                "headers": [], "cookies": [],
                "content": {"size": 0, "mimeType": "", "text": ""},
                "redirectURL": ""
            },
            "cache": {},
            "timings": {"send": 0, "wait": 0, "receive": 0}
        }
        if body_text:
            entry["request"]["postData"] = {
                "mimeType": headers.get("Content-Type","application/x-www-form-urlencoded"),
                "text": body_text
            }
        self.seq2idx[seq] = len(self.log["log"]["entries"])
        self.log["log"]["entries"].append(entry)

    def on_resp(self, seq, status, headers, text):
        if not HAR_ENABLE: return
        idx = self.seq2idx.get(seq)
        if idx is None: return
        ent = self.log["log"]["entries"][idx]
        ent["response"]["status"] = int(status or 0)
        ent["response"]["headers"] = self._headers_list(headers or {})
        ent["response"]["content"]["text"] = text or ""
        ent["response"]["content"]["size"] = len(text or "")
        ent["response"]["content"]["mimeType"] = (headers or {}).get("content-type","")

        # incremental flush (안전)
        try:
            pathlib.Path(self.path).write_text(json.dumps(self.log, ensure_ascii=False, indent=2), encoding="utf-8")
        except: pass

HAR = _HarRec()
atexit.register(lambda: pathlib.Path(HAR.path).write_text(json.dumps(HAR.log, ensure_ascii=False, indent=2), encoding="utf-8") if HAR_ENABLE else None)
# ===================================================================

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

# --- ADD: payment/price/order 감지 시 자동 홀드 -------------------------------
@trace_step("arm_payment_hold")
async def arm_payment_hold(page: Page):
    if not PAY_STAY:
        return
    import re, asyncio
    RX = re.compile(r"/payment\b", re.I)

    # (안전) 네비게이션이든 리소스 요청이든 한쪽만 걸려도 홀드
    def _on_req(req):
        try:
            u = req.url or ""
            if "filmonestop.maketicket.co.kr" in u and RX.search(u):
                try:
                    page.off("request", _on_req)
                except:
                    pass
                asyncio.create_task(hold_at_payment(page))
        except:
            pass

    page.on("request", _on_req)

    # 가능하면 프레임 네비게이션도 감시 (미지원 환경은 조용히 패스)
    try:
        def _on_nav(frame):
            try:
                u = frame.url or ""
                if "filmonestop.maketicket.co.kr" in u and RX.search(u):
                    try:
                        page.off("framenavigated", _on_nav)
                    except:
                        pass
                    asyncio.create_task(hold_at_payment(page))
            except:
                pass
        page.on("framenavigated", _on_nav)
    except:
        pass
# ---------------------------------------------------------------------------


async def ensure_scope_or_spawn(scope_or_page, prodSeq: str, sdSeq: str):
    """
    filmonestop booking scope 확보 전략:
      1) 이미 떠 있으면 그대로 사용
      2) resMain이면 예매/Next 버튼 눌러 iframe 유도
      3) 최후: 같은 컨텍스트에서 onestop 경로들을 잠깐 열어 쿠키/스코프 시드 (booking → rs → rs/seat)
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
        if page and "biff.maketicket.co.kr" in (getattr(page, "url", "") or ""):
            sc = await ensure_booking_iframe(page, prodSeq=prodSeq, sdSeq=sdSeq)
            if sc:
                return sc
    except:
        pass

    # 3) 최후 수단: 같은 컨텍스트에서 booking → rs → rs/seat 순서로 열어서 scope 확보
    try:
        page = getattr(scope_or_page, "page", None) or scope_or_page
        ctx  = getattr(page, "context", None)
        if ctx and prodSeq and sdSeq:
            for path in [
                f"{FILM_ONESTOP_HOST}/ko/onestop/booking?prodSeq={prodSeq}&sdSeq={sdSeq}",
                f"{FILM_ONESTOP_HOST}/ko/onestop/rs?prodSeq={prodSeq}&sdSeq={sdSeq}",
                f"{FILM_ONESTOP_HOST}/ko/onestop/rs/seat?prodSeq={prodSeq}&sdSeq={sdSeq}",
            ]:
                tmp = await ctx.new_page()
                keep = False
                try:
                    await tmp.goto(path, wait_until="domcontentloaded", timeout=15000)
                    sc = await ensure_filmonestop_scope(tmp)
                    if sc:
                        keep = True
                        return sc
                finally:
                    if not keep:
                        try:
                            await tmp.close()
                        except:
                            pass
    except:
        pass
    return None

@trace_step("seat_counts_via_blocksummary2")
async def seat_counts_via_blocksummary2(scope0, prodSeq, sdSeq,
                                        chnlCd="WEB", csrfToken="", perfDate=""):
    REFS = build_onestop_referers(scope0, str(prodSeq), str(sdSeq))
    H_RS = _ref_headers(REFS, "rs")
    if csrfToken:
        H_RS["X-CSRF-TOKEN"] = csrfToken

    js = await post_api(scope0, "/rs/blockSummary2", {
        "langCd": "ko", "csrfToken": csrfToken,
        "prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
        "chnlCd": chnlCd, "perfDate": perfDate or ""
    }, extra_headers=H_RS, allow_no_csrf=(ALLOW_NO_CSRF or (not csrfToken)))

    # 응답 형태: dict에 data/list/rows/… 또는 리스트
    if isinstance(js, dict):
        arr = js.get("data") or js.get("list") or js.get("rows") or js.get("result") or []
    else:
        arr = js

    total, remain = 0, 0
    for it in (arr or []):
        if not isinstance(it, dict):
            continue
        t = _coalesce_int(
            it,
            "seatCnt","totalSeatCnt","totSeatCnt",
            "saleSeatCnt","rendrSeatCnt",
            "totalPersonCnt","admissionTotalPersonCnt",
            default=0
        )
        a = _coalesce_int(
            it,
            "remainSeatCnt","restSeatCnt","availableSeatCnt","ableSeatCnt",
            "rmnSeatCnt","admissionAvailPersonCnt",
            default=0
        )
        total  += max(t, 0)
        remain += max(a, 0)

    return total, remain, {"NRS": remain}, "NRS"

# --- add: make blockSummary2 results uniform (tuple|dict → dict) ---
def _coerce_summary(summary):
    # summary가 (total, remain, by, plan) 튜플이면 dict로 변환
    if isinstance(summary, tuple) and len(summary) >= 4:
        t, r, by, plan = summary[:4]
        return {"total": t, "remain": r, "by": by, "plan": plan}
    return summary  # 이미 dict면 그대로

        
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

# === REPLACE ENTIRE FUNCTION: _harvest_booking_ctx ===
async def _harvest_booking_ctx(page) -> dict:
    """현재 페이지에서 prodSeq/sdSeq/chnlCd/csrf/sdCode/perfDate를 안전하게 긁어온다."""
    JS = r"""
    () => {
      const pick = (...sels) => {
        for (const s of sels) {
          const el = document.querySelector(s);
          if (el) return (el.value || el.getAttribute('content') || el.textContent || '').trim();
        }
        return '';
      };
      const qs = new URL(location.href).searchParams;
      const out = {
        prodSeq:  pick('#prodSeq','input[name="prodSeq"]','input[name="prod_seq"]') || qs.get('prodSeq') || '',
        sdSeq:    pick('#sdSeq','input[name="sdSeq"]','input[name="sd_seq"]')       || qs.get('sdSeq')   || '',
        chnlCd:   pick('#chnlCd','input[name="chnlCd"]','input[name="chnl_cd"]')    || 'WEB',
        perfDate: (pick('#perfDate') || qs.get('perfDate') || '').replace(/-/g,''),
        csrf:     pick('#csrfToken','meta[name="csrf-token"]') || (window._csrf || ''),
        sdCode:   pick('#sdCode','input[name="sdCode"]')        || qs.get('sdCode') || '',
      };
      if (!out.prodSeq || !out.sdSeq) {
        const html = document.documentElement.innerHTML;
        const m1 = html.match(/prodSeq["']?\s*[:=]\s*["']?(\d+)/i); if (m1) out.prodSeq = m1[1];
        const m2 = html.match(/sdSeq["']?\s*[:=]\s*["']?(\d+)/i);   if (m2) out.sdSeq   = m2[1];
      }
      return out;
    }
    """
    try:
        raw = await page.evaluate(JS) or {}
        csrf = raw.get("csrf") or raw.get("csrfToken") or ""
        return {
            "prodSeq":   str(raw.get("prodSeq","")),
            "sdSeq":     str(raw.get("sdSeq","")),
            "chnlCd":    (raw.get("chnlCd") or "WEB") or "WEB",
            "perfDate":  raw.get("perfDate",""),
            "csrf":      csrf,
            "csrfToken": csrf,   # ← alias (호출부 호환)
            "sdCode":    raw.get("sdCode",""),
        }
    except Exception as e:
        dlog(f"[_harvest_booking_ctx] {e}")
        return {"prodSeq":"","sdSeq":"","chnlCd":"WEB","perfDate":"","csrf":"","csrfToken":"","sdCode":""}
# === HAR-FIRST: loader & indexer ============================================
import glob, json, pathlib, re, urllib.parse, time
HAR_CACHE = {"built": 0, "by_path": [], "forms": []}

def _har_paths_from_env() -> list[str]:
    raw = os.getenv("HAR_PATHS", "")
    if not raw: return []
    out = []
    for piece in raw.split(";"):
        piece = piece.strip()
        if not piece: continue
        # 파일이면 그대로, 디렉토리면 *.har
        if any(ch in piece for ch in ["*", "?", "["]):
            out.extend(glob.glob(piece))
        else:
            p = pathlib.Path(piece)
            if p.is_file(): out.append(str(p))
            elif p.is_dir(): out.extend(glob.glob(str(p / "*.har")))
    # 중복 제거 + 최신 우선
    return sorted(set(out), key=lambda s: (pathlib.Path(s).stat().st_mtime if pathlib.Path(s).exists() else 0), reverse=True)

def _kv_from_query(url: str) -> dict:
    try:
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(url).query or "")
        flat = {k:(v[0] if isinstance(v, list) and v else "") for k,v in qs.items()}
        return flat
    except: return {}

def _form_from_entry(e: dict) -> dict:
    req = e.get("request", {})
    method = (req.get("method") or "GET").upper()
    url = req.get("url") or ""
    form = {}
    if method == "POST":
        postData = req.get("postData") or {}
        # application/x-www-form-urlencoded
        if isinstance(postData, dict) and postData.get("params"):
            for kv in postData["params"]:
                form[kv.get("name")] = kv.get("value") or ""
        elif isinstance(postData, dict) and isinstance(postData.get("text"), str):
            txt = postData["text"]
            # 폼/JSON 모두 처리
            if "&" in txt and "=" in txt:
                try:
                    form.update({k:v[0] if v else "" for k,v in urllib.parse.parse_qs(txt).items()})
                except: pass
            else:
                try:
                    js = json.loads(txt)
                    if isinstance(js, dict): 
                        # 중첩 평탄화
                        for k,v in js.items():
                            if isinstance(v, (str,int,float)): form[str(k)] = str(v)
                except: pass
    # GET 쿼리도 병합
    form.update(_kv_from_query(url))
    return form

def _csrf_from_headers(req: dict) -> str:
    h = { (k or "").lower(): v for k,v in (req.get("headers") or []) if isinstance(k, str) }
    # X-CSRF-TOKEN 가장 우선, 없으면 Cookie에서 XSRF/CSRF 토큰 추출
    tok = h.get("x-csrf-token") or ""
    if tok: return tok
    cookie = h.get("cookie") or ""
    m = re.search(r"(?:XSRF-TOKEN|CSRF-TOKEN|X-CSRF-TOKEN)=([^;]+)", cookie)
    return m.group(1) if m else ""

def build_har_index(force: bool = False):
    if HAR_CACHE["built"] and not force:
        return HAR_CACHE
    paths = _har_paths_from_env()
    by_path = []   # [(ts, host, path, method, status, form, csrf, referer)]
    forms  = []    # 원본 폼만 모아두기
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8", errors="ignore") as f:
                har = json.load(f)
        except Exception:
            continue
        entries = (har.get("log", {}) or {}).get("entries", [])
        for e in entries:
            req, res = e.get("request", {}), e.get("response", {})
            url   = req.get("url") or ""
            host  = urllib.parse.urlparse(url).netloc
            path  = urllib.parse.urlparse(url).path
            meth  = (req.get("method") or "GET").upper()
            stat  = int(res.get("status") or 0)
            ref   = ""
            for k,v in (req.get("headers") or []):
                if str(k).lower() == "referer": ref = v
            form  = _form_from_entry(e)
            csrf  = _csrf_from_headers(req)
            ts    = e.get("startedDateTime") or ""
            by_path.append( (ts, host, path, meth, stat, form, csrf, ref) )
            if form: forms.append(form)
    # 최신순으로 정렬
    by_path.sort(key=lambda r: r[0], reverse=True)
    HAR_CACHE["built"] = int(time.time())
    HAR_CACHE["by_path"] = by_path
    HAR_CACHE["forms"]   = forms
    return HAR_CACHE

def har_params_for(sd: str|int) -> dict:
    """sdCode 또는 sdSeq로 추정되는 값 묶음을 HAR에서 뽑는다."""
    sd = str(sd)
    cache = build_har_index()
    out = {}
    for ts, host, path, meth, stat, form, csrf, ref in cache["by_path"]:
        if "maketicket.co.kr" not in host:
            continue
        # sdCode 또는 sdSeq 일치하는 최근 폼 찾기
        if (form.get("sdCode") == sd) or (form.get("sdSeq") == sd):
            # 핵심 파라미터 묶음 확보
            for k in ("prodSeq","sdSeq","perfDate","sdCode","chnlCd","saleTycd","saleCondNo","planTypeCd","seatTypeCode"):
                if form.get(k): out.setdefault(k, str(form.get(k)))
            if csrf: out.setdefault("csrfToken", csrf)
            # referer도 힌트로 저장
            if ref: out.setdefault("_Referer", ref)
            break
    return out
# ===================================================================

def _coalesce_int(d: dict, *keys, default=0):
    for k in keys:
        try:
            v = d.get(k)
            if v is None: 
                continue
            return int(v)
        except:
            continue
    return int(default)



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

    def _form():
        base = {
            "prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
            "perfDate": perfDate or seq2date.get(int(sdSeq), ""),
            "sdCode": sdCode,
            "saleTycd": saleTycd, "saleCondNo": saleCondNo,
            "jType": "N", "rsrvStep": "TKT", "langCd": "ko",
            "csrfToken": csrfToken or csrf0 or ""
        }
        # [HAR] tickettype 폼 보강
        if os.getenv("USE_HAR", "0") == "1":
            try:
                h = har_params_for(sdSeq)
            except Exception:
                h = {}
            for k in ("saleTycd","saleCondNo","planTypeCd","seatTypeCode","chnlCd","sdCode","perfDate"):
                if h.get(k):
                    base[k] = h[k]
        return base

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

    aggs = [s for s in seat_list if (str(s.get("seatTypeCd") or s.get("seat_type_cd") or "").upper() == "NRS"
                                     and (s.get("seatNo") or s.get("seat_no") or "") == "")]
    if aggs:
        s = aggs[0]
        sold   = int(s.get("admissionPersonCnt") or s.get("saleCnt") or 0)
        remain = int(s.get("admissionAvailPersonCnt") or s.get("remainSeatCnt") or s.get("restSeatCnt") or 0)
        total_cand = (s.get("admissionTotalPersonCnt") or s.get("totalPersonCnt") or 0)
        total = max(total, int(total_cand) if total_cand else (remain + sold))
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

# --- ADD: NRS/ALL fallback via blockSummary2 ---------------------------------
@trace_step("seat_counts_via_blocksummary2")
async def seat_counts_via_blocksummary2(scope0, prodSeq, sdSeq, chnlCd="WEB", csrfToken="", perfDate=""):
    """
    blockSummary2 응답(블록/존 요약)만으로 총좌석/잔여를 집계.
    plan이 ALL/NRS이거나 tickettype이 실패할 때 사용.
    """
    REFS = build_onestop_referers(scope0, str(prodSeq), str(sdSeq))
    ORI  = _onestop_origin_from_context(scope0)
    H_RS = {"Referer": REFS.get("rs", REFS.get("prod", "")), "Origin": ORI}

    js = await post_api(scope0, "/rs/blockSummary2", {
        "langCd": "ko", "csrfToken": csrfToken,
        "prodSeq": str(prodSeq), "sdSeq": str(sdSeq),
        "chnlCd": chnlCd, "perfDate": perfDate or ""
    }, extra_headers=H_RS)

    # 응답 형태 수용: list 또는 {data:[...]}
    arr = js.get("data") if isinstance(js, dict) else js
    total, remain = 0, 0
    by = {}
    for it in (arr or []):
        if not isinstance(it, dict): 
            continue
        try:
            t = int(it.get("seatCnt") or it.get("totalSeatCnt") or it.get("totSeatCnt") or 0)
        except: 
            t = 0
        try:
            a = int(
                it.get("remainSeatCnt") or it.get("restSeatCnt") or 
                it.get("availableSeatCnt") or it.get("ableSeatCnt") or 0
            )
        except:
            a = 0
        total  += max(t, 0)
        remain += max(a, 0)
    if total or remain:
        by["NRS"] = remain
    return total, remain, by, "NRS"
# ---------------------------------------------------------------------------


@trace_step("fetch_seat_summary")
async def fetch_seat_summary(scope0, prodSeq, sdSeq,
                             chnlCd="WEB", saleTycd="SALE_NORMAL",
                             csrfToken="", saleCondNo="1", perfDate=""):
    scope0 = await ensure_scope_or_spawn(scope0, str(prodSeq), str(sdSeq)) or scope0
    if not scope0:
        raise RuntimeError("filmonestop scope not found (cannot proceed)")

    REFS   = build_onestop_referers(scope0, str(prodSeq), str(sdSeq))
    H_RS   = _ref_headers(REFS, "rs")
    H_SEAT = _ref_headers(REFS, "seat")

    # 0) CSRF 확보 및 주입
    if not csrfToken:
        csrfToken = await ensure_csrf_token_for(scope0, str(prodSeq), str(sdSeq))
    if csrfToken:
        H_RS["X-CSRF-TOKEN"] = csrfToken
        H_SEAT["X-CSRF-TOKEN"] = csrfToken

    # 0-1) HAR 주입(있을 때만)
    if os.getenv("USE_HAR", "0") == "1":
        try:
            har = har_params_for(sdSeq)
        except Exception:
            har = {}
        perfDate  = perfDate  or har.get("perfDate")  or ""
        csrfToken = csrfToken or har.get("csrfToken") or ""
        if har.get("_Referer"):
            from urllib.parse import urlparse
            ref = har["_Referer"].rstrip("/")
            ori = urlparse(ref)
            H_RS   = {"Referer": ref, "Origin": f"{ori.scheme}://{ori.netloc}"}
            H_SEAT = dict(H_RS)

    # 1) plan_type 확인
    plan_type = ""
    try:
        base = await post_api(scope0, "/seat/GetRsSeatBaseMap",
            {"prod_seq": str(prodSeq), "sd_seq": str(sdSeq),
             "chnl_cd": chnlCd, "sale_tycd": saleTycd,
             "csrfToken": csrfToken},
            extra_headers=H_SEAT,
            allow_no_csrf=(ALLOW_NO_CSRF or (not csrfToken)))
        if isinstance(base, dict):
            plan_type = base.get("plan_type") or base.get("planType") or ""
    except Exception as e:
        dlog(f"[seat] GetRsSeatBaseMap failed: {e}")

    upper_pt = (plan_type or "").upper()

    # 2) 자유석(NRS/FREE) → blockSummary2 우선
    if upper_pt in ("NRS", "FREE"):
        try:
            blk = await post_api(scope0, "/rs/blockSummary2",
                {"langCd":"ko","csrfToken":csrfToken,"prodSeq":str(prodSeq),
                 "sdSeq":str(sdSeq),"chnlCd":chnlCd,"perfDate":perfDate},
                extra_headers=H_RS,
                allow_no_csrf=(ALLOW_NO_CSRF or (not csrfToken)))
            summ = blk.get("summary") if isinstance(blk, dict) else {}
            if isinstance(summ, list) and summ:
                summ = summ[0]

            remain = _coalesce_int(
                summ,
                "admissionAvailPersonCnt", "remainSeatCnt", "restSeatCnt",
                "availableSeatCnt", "ableSeatCnt", "rmnSeatCnt",
                default=0
            )
            total = _coalesce_int(
                summ,
                "admissionTotalPersonCnt", "seatCnt", "totalSeatCnt", "totSeatCnt",
                "saleSeatCnt", "rendrSeatCnt", "totalPersonCnt", "admissionTotalCnt",
                default=0
            )
            sold = _coalesce_int(
                summ,
                "admissionPersonCnt", "soldSeatCnt", "sellSeatCnt", "saleCnt",
                default=0
            )

            # 총이 0인데 잔여/판매가 있으면 보정
            if total == 0 and (remain > 0 or sold > 0):
                total = remain + sold

            # summary도 0이면 블록 전수 집계 폴백
            if total == 0 and remain == 0:
                total, remain, by, pt = await seat_counts_via_blocksummary2(
                    scope0, prodSeq, sdSeq, chnlCd=chnlCd,
                    csrfToken=csrfToken, perfDate=perfDate
                )
                return total, remain, by, (pt or upper_pt or "NRS")

            return total, remain, {"NRS": remain}, (upper_pt or "NRS")
        except Exception as e:
            dlog(f"[seat] NRS summary failed: {e}")

    # 3) 지정석(RS) 또는 불명 → statusList 기반
    try:
        lst = await _fetch_seat_status_list(
            scope0, prodSeq, sdSeq,
            chnlCd=chnlCd, csrfToken=csrfToken,
            extra_headers=H_SEAT,
            allow_no_csrf=(ALLOW_NO_CSRF or (not csrfToken))
        )
        lst = _normalize_seat_list(lst)
        total, remain, by = _count_seats(lst, AVAILABLE_CODES)
        if total or remain:
            return total, remain, by, (upper_pt or "RS")
    except Exception as e:
        dlog(f"[seat] statusList failed: {e}")

    # 4) 폴백: blockSummary2 전수 집계
    try:
        total, remain, by, pt = await seat_counts_via_blocksummary2(
            scope0, prodSeq, sdSeq, chnlCd=chnlCd,
            csrfToken=csrfToken, perfDate=perfDate
        )
        if total or remain:
            return total, remain, by, (pt or upper_pt or "ALL")
    except Exception as e:
        dlog(f"[rs] blockSummary2 fallback failed: {e}")

    # 5) 최후 폴백: prodSummary
    try:
        ps = await post_api(scope0, "/rs/prodSummary",
            {"langCd":"ko","csrfToken":csrfToken,"prodSeq":str(prodSeq),
             "sdSeq":str(sdSeq),"chnlCd":chnlCd,"perfDate":perfDate},
            extra_headers=H_RS,
            allow_no_csrf=(ALLOW_NO_CSRF or (not csrfToken)))
        summ = ps.get("summary") if isinstance(ps, dict) else {}
        if isinstance(summ, list) and summ:
            summ = summ[0]

        total = _coalesce_int(
            summ, "rendrSeatCnt","saleSeatCnt","seatCnt","totalSeatCnt","totSeatCnt",
            default=0
        )
        remain = _coalesce_int(
            summ, "restSeatCnt","remainSeatCnt","availableSeatCnt","ableSeatCnt",
            "rmnSeatCnt","admissionAvailPersonCnt",
            default=0
        )
        return total, remain, {"NRS": remain}, (upper_pt or "ALL")
    except Exception as e:
        dlog(f"[rs] prodSummary failed: {e}")

    return 0, 0, {}, (upper_pt or "ALL")


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
    GetRsSeatStatusList 응답에서 '구매 가능(SS01000)' 좌석 1개 선택.
    중앙/중후열 우선 스코어 적용.
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

        # SS01000 only
        candidates = [
            (str(d.get("seat_id")), str(d.get("seat_class_seq") or "1"))
            for d in js
            if (d.get("use_yn") == "Y") and (d.get("seat_status_cd") == "SS01000") and d.get("seat_id")
        ]
        if not candidates:
            return (None, None)

        # 중앙 선호 스코어
        def _num(x):
            try: return int(x)
            except: return 0
        cols = [_num(d.get("col_no") or d.get("seat_col") or d.get("seat_pos_x")) for d in js if d.get("use_yn") == "Y"]
        rows = [_num(d.get("row_no") or d.get("seat_row") or d.get("seat_pos_y")) for d in js if d.get("use_yn") == "Y"]
        col_mid = (min(cols)+max(cols))//2 if cols else 0
        row_mid = (min(rows)+max(rows))//2 if rows else 0

        def _score(t):
            sid, _ = t
            d = next((x for x in js if str(x.get("seat_id")) == sid), None)
            if not isinstance(d, dict): return 10**9
            c = _num(d.get("col_no") or d.get("seat_col") or d.get("seat_pos_x"))
            r = _num(d.get("row_no") or d.get("seat_row") or d.get("seat_pos_y"))
            return abs(c - col_mid) + abs(r - row_mid)

        try:
            candidates.sort(key=_score)
        except Exception:
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
            if st in {"SS01000"}:
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
                   timeout_ms: int = 15000, extra_headers: dict | None = None,
                   allow_no_csrf: bool = False):
    # rs 계열 & prod 제외는 무조건 토큰 필요
    needs_csrf = (
        path.startswith("/api/v1/rs/") or path.startswith("/rs/") or
        path.startswith("/api/v1/seat/") or path.startswith("/seat/")
    ) and not path.endswith("/prod")
    # 읽기성 엔드포인트(또는 토큰 불명 시) 강제 허용 옵션
    if allow_no_csrf:
        needs_csrf = False
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




# === REPLACE ENTIRE FUNCTION: fetch_json (with NETLOG + HAR) ===
@trace_step("fetch_json")
async def fetch_json(scope_or_page, *args, **kwargs):
    import json

    # 하위호환 인자 정규화
    if "timeout_ms" in kwargs and "timeout" not in kwargs:
        kwargs["timeout"] = kwargs.pop("timeout_ms")
    if "extra_headers" in kwargs and "headers" not in kwargs:
        kwargs["headers"] = kwargs.pop("extra_headers")
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
    body_text = ""
    if data is not None:
        fetch_kwargs["data"] = data
        if isinstance(data, (bytes, bytearray)):
            try: body_text = data.decode("utf-8", "ignore")
            except: body_text = ""
        else:
            body_text = str(data)

    # --- NETLOG + HAR: 요청 기록 ---
    seq = _netlog_req(url, base_hdrs, {} if isinstance(data, str) else {}, body_text)
    if HAR_ENABLE:
        HAR.on_req(seq, url, method, base_hdrs, body_text)

    # 호출
    resp = await req.fetch(url, **fetch_kwargs)
    txt = await resp.text()

    # --- NETLOG + HAR: 응답 기록 ---
    try:
        _netlog_resp(seq, url, resp.status, txt)
    except Exception:
        pass
    if HAR_ENABLE:
        try:
            HAR.on_resp(seq, resp.status, dict(resp.headers), txt)
        except Exception:
            pass

    if resp.status < 200 or resp.status >= 300:
        raise RuntimeError(f"{resp.status} {url} — {txt[:200]}")

    # 안전 JSON 파싱
    ctype = (resp.headers.get("content-type") or "").lower()
    s = (txt or "").lstrip()
    if "application/json" in ctype or (s.startswith("{") or s.startswith("[")):
        try:
            return json.loads(txt)
        except Exception:
            return txt
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

# ── 공통 알림/확인 모달 처리 ───────────────────────────────────────────────
@trace_step("clear_any_alert")
async def clear_any_alert(scope):
    import re
    try:
        # role 기반 우선 시도
        for rx in [re.compile(r"^\s*확인\s*$"), re.compile(r"^\s*OK\s*$", re.I),
                   re.compile(r"^\s*(예|네)\s*$")]:
            try:
                btn = scope.get_by_role("button", name=rx)
                if await btn.count():
                    try: await btn.first.scroll_into_view_if_needed()
                    except: pass
                    try:
                        await btn.first.click(timeout=500)
                        await scope.wait_for_timeout(150)
                        return True
                    except: pass
            except: pass

        # fallback: 흔한 셀렉터들
        try:
            sel = scope.locator(".layer .btn_confirm, .modal .btn_confirm, .popup .btn_confirm, .dialog .btn_confirm, button")
            sel = sel.filter(has_text=re.compile(r"(확인|OK|예|네)", re.I))
            if await sel.count():
                try: await sel.first.scroll_into_view_if_needed()
                except: pass
                try:
                    await sel.first.click(timeout=500)
                    await scope.wait_for_timeout(150)
                    return True
                except: pass
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
async def _fetch_seat_status_list(pop, prodSeq, sdSeq, chnlCd="WEB", csrfToken="", extra_headers=None, **kwargs):
    # 1) snake_case
    try:
        js = await post_api(pop, "/seat/GetRsSeatStatusList", {
            "prod_seq": str(prodSeq), "sd_seq": str(sdSeq), "chnl_cd": chnlCd,
            "timeStemp": "", "csrfToken": csrfToken or ""
        }, extra_headers=extra_headers, **kwargs)
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
        }, extra_headers=extra_headers, **kwargs)
        if isinstance(js, dict) and js.get("resultCode") not in (None, "0000"):
            dlog(f"[SEAT] camelCase non-0000: {js.get('resultCode')} {js.get('resultMessage')}")
        lst = (js.get("list") if isinstance(js, dict) else js) or []
        return lst if isinstance(lst, list) else []
    except Exception as e:
        dlog(f"[SEAT] camelCase failed: {e}")
        return []

# === REPLACE ENTIRE FUNCTION: _count_seats ===
def _count_seats(lst, available_codes={"SS01000"}):
    total, remain, by = 0, 0, {}
    for it in (lst or []):
        # ★ 핵심 가드: dict 아니면 스킵 (str/None/숫자/리스트 조각 등 전부 무시)
        if not isinstance(it, dict):
            continue
        # 집계 응답(상태별 카운트)은 useYn이 아예 없을 수 있음 → 없으면 통과
        use = it.get("useYn") or it.get("use_yn")
        if use is not None and str(use).upper() not in ("Y","YES","TRUE","1"):
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

async def is_seat_page(page) -> bool:
    try:
        path = await page.evaluate("location.pathname")
        if "/onestop/rs/seat" in path or "/rs/seat" in path:
            return True
        # 좌석 SVG/캔버스 존재로도 보조 판정
        return await page.locator("svg, canvas, .seat-map, #seatMap").count() > 0
    except Exception:
        return False

async def is_zone_page(page) -> bool:
    try:
        path = await page.evaluate("location.pathname")
        return "/onestop/rs/zone" in path or "/rs/zone" in path
    except Exception:
        return False

async def _click_any_available_seat(p: Page) -> bool:
    # 가용좌석 우선시
    seat_sels = [
        "[data-seat-state='A']",
        "[data-usable='Y']",
        "[data-status='A']",
        ".seat.available",
        "g.seat[data-status='A']",
        "g[id*='SEAT'][data-state='A']",
        "[data-seat]"
    ]
    for s in seat_sels:
        loc = p.locator(s).first
        try:
            if await loc.count() > 0:
                await loc.click(timeout=1000)
                return True
        except:  # 다음 셀렉터 시도
            pass
    return False

async def _click_zone_with_remain(p: Page, params: Dict[str, str]) -> bool:
    """blockSummary2의 잔여 기준으로 아무 존이나 선택 시도 → 실패 시 첫 존 클릭"""
    try:
        bs = await fetch_json(
            p,
            "https://filmonestopapi.maketicket.co.kr/rs/blockSummary2",
            {"prodSeq": params["prodSeq"], "sdSeq": params["sdSeq"], "csrfToken": params["csrfToken"]}
        )
    except:
        bs = None

    target_names = []
    # 요약에서 잔여 있는 존 후보 뽑기
    try:
        for b in (bs or {}).get("blockList") or []:
            name = str(b.get("blockNm") or b.get("zoneNm") or "").strip()
            rmn = b.get("rmnSeatCnt")
            try:
                rmn = int(str(rmn).replace(",", ""))
            except:
                rmn = None
            if name and (rmn is None or rmn > 0):
                target_names.append(name)
    except:
        pass

    # DOM에서 클릭
    zone_try = []
    for nm in target_names:
        # 이름으로 찾기
        zone_try += [f"text=^{nm}$", f"[data-zone-name='{nm}']"]
    # 이름 매칭 실패 시 구조 기반
    zone_try += [
        "[data-zone]:not([data-remain='0'])",
        "svg [data-zone]",
        "svg [data-zoneid]",
        "g[id^=zone]:not(.soldout)",
        "[id^=zoneList] [role='button'], [id^=zoneList] [data-zone]"
    ]

    for sel in zone_try:
        loc = p.locator(sel).first
        try:
            if await loc.count() > 0:
                await loc.click(timeout=1000)
                return True
        except:
            continue
    return False

async def _click_next_in_ctx(p: Page) -> bool:
    clicked = await click_like(p, RX_NEXT)
    if clicked:
        return True
    f = await find_seat_frame(p)
    if f:
        return await click_like(f, RX_NEXT)
    return False


async def fcfs_chain_then_next(p: Page, sd_code: str, params: Dict[str, str]) -> bool:
    prodSeq = params.get("prodSeq"); sdSeq = params.get("sdSeq")
    perfDate = params.get("perfDate"); csrf = params.get("csrfToken")
    ch = params.get("chnlCd","WEB")
    seat_type = (params.get("seatTypeCode") or "RS").upper()

    # 먼저 현재 화면이 좌석/존인지 확인
    if await is_seat_page(p):
        print("🟡 좌석맵 감지 → 좌석 1개 선택 후 Next")
        picked = await _click_any_available_seat(p)
        if not picked:
            print("⚠️ 좌석 선택 실패 → Next만 시도")
        return await _click_next_in_ctx(p)

    if await is_zone_page(p):
        print("🟡 존 선택 단계 감지 → 잔여 있는 존 선택 시도")
        ok = await _click_zone_with_remain(p, params)
        if not ok:
            print("⚠️ 잔여존 클릭 실패 → 임의 존/다음 단계 시도")
        # 좌석맵 로딩 잠깐 대기
        try:
            await p.wait_for_timeout(200)
        except: pass
        # 좌석 선택 시도
        if await is_seat_page(p):
            picked = await _click_any_available_seat(p)
            if not picked:
                print("⚠️ 좌석 선택 실패 → Next만 시도")
        return await _click_next_in_ctx(p)

    # ----- 여기까지도 아니면 NRS/기타 단계 -----

    # 수량=1 힌트(서버가 무시해도 무방)
    try:
        await fetch_json(
            p,
            "https://filmonestopapi.maketicket.co.kr/api/v1/rs/seatStateInfo",
            {"prodSeq": prodSeq, "sdSeq": sdSeq, "seatId": "1", "csrfToken": csrf}
        )
    except:
        pass

    # RS(좌석제)만 결제 힌트 프리페치 (네가 하던대로 유지)
    if seat_type == "RS":
        try:
            await payment_hint_chain(p, sd_code, dict(params))
        except:
            pass

    # Next
    return await _click_next_in_ctx(p)


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
        "select[name='sel0']",
        "#sel0",
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

# 티켓 유형(성인/일반 등) 1개 선택 보조
@trace_step("select_first_ticket_type")
async def select_first_ticket_type(scope):
    import re
    # 1) 라디오/체크박스형 우선
    for sel in [
        "input[type='radio'][name='ticketType']:not([disabled])",
        "input[type='radio'][name*='ticketType' i]:not([disabled])",
        "input[type='radio'][name='tkttypSeq']:not([disabled])",
        "input[type='radio'][name='priceSeq']:not([disabled])",
        "input[type='checkbox'][name='ticketType']:not([disabled])",
    ]:
        try:
            loc = scope.locator(sel)
            if await loc.count():
                try:
                    await loc.first.check()
                except:
                    await loc.first.click(timeout=400)
                await scope.wait_for_timeout(120)
                return True
        except: pass

    # 2) 버튼/라벨형 텍스트 기반
    try:
        btn = scope.locator("button, a, label").filter(has_text=re.compile(r"(성인|일반|Adult)", re.I))
        if await btn.count():
            await btn.first.click(timeout=500)
            await scope.wait_for_timeout(120)
            return True
    except: pass

    return False


# === 가격화면 강제 동작: qty=1 설정 + Next 클릭 ====================
@trace_step("enter_price_and_next")
async def enter_price_and_next(popup_or_scope):
    # scope가 Frame/Locator/Popup 무엇이든 대응
    scope = popup_or_scope
    page = getattr(scope, "page", None) or getattr(scope, "context", None) or getattr(scope, "owner", None)

    # 포커스 보장 (팝업/프레임 전환 대비)
    try:
        if hasattr(scope, "bring_to_front"):
            await scope.bring_to_front()
        if hasattr(scope, "evaluate"):
            await scope.evaluate("window.focus && window.focus()")
    except:
        pass

    # (0) 프레임/스코프 정규화: 예약 전용 프레임을 최우선으로
    try:
        page0 = getattr(scope, "page", None) or scope
        fb = find_booking_scope(page0)
        frame = fb or getattr(scope, "main_frame", None) or getattr(scope, "frame", None) or scope
    except:
        frame = getattr(scope, "main_frame", None) or getattr(scope, "frame", None) or scope

    # (1) qty=1 세팅 시도(여러 경로로 전부)
    async def _set_qty_1():
        # 1-1) id 직격
        try:
            sel = frame.locator("#volume_1_1")
            if await sel.count():
                try:
                    try:
                        await sel.first.select_option("1")
                    except:
                        await sel.first.select_option(index=1)  # 두 번째 옵션(=1매)
                    eh = await sel.first.element_handle()
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

        # 1-2) '일반' 행 안의 volume_* 탐색
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

        # 1-3) 다른 select 후보 전수 (사이트 고유 sel0 포함)
        for css in [
            "select[name='sel0']",
            "#sel0",
            "select[name='rsVolume']",
            "#rsVolume",
            "select[name='selectedTicketCount']",
            "#selectedTicketCount",
            "select[id^='volume_']",
        ]:
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

        # 1-4) 마지막: 히든필드 직업 + 이벤트
        try:
            ok = await frame.evaluate("""()=>{
                const fire=(el,t)=>el&&el.dispatchEvent(new Event(t,{bubbles:true}));
                const set=(name,val)=>{
                    const el=document.querySelector(`[name="${name}"]`)||document.getElementById(name);
                    if(!el) return false; el.value=val; fire(el,'input'); fire(el,'change'); return true;
                };
                return set('selectedTicketCount','1')||set('rsVolume','1')||set('sel0','1');
            }""")
            if ok:
                await frame.wait_for_timeout(100)
                return True
        except:
            pass

        return False

    qty_ok = await _set_qty_1()

    # (2) 티켓 유형(성인/일반 등) 1개 선택 (필수 사이트 대응)
    try:
        await select_first_ticket_type(frame)
    except:
        pass

    # (2-b) Next 활성화 방해 요소 제거(오버레이/딤)
    try:
        await frame.evaluate("""()=>{
            const hide=(el)=>{ if(!el) return; el.dataset._prevDisplay = el.style.display; el.style.display='none'; };
            ['.dim','.__dim__','.overlay','.modal-backdrop','.loading','#loading','[aria-hidden="true"].modal']
              .forEach(sel => document.querySelectorAll(sel).forEach(hide));
        }""")
    except:
        pass

    # (3) Next 강제 클릭 (disabled 우회 + JS 강클릭 + 모달 자동 닫기)
    async def _try_next():
        import re

        # role 기반 우선
        try:
            btn = frame.get_by_role("button", name=re.compile(r"(다음|다음으로|다음 단계|결제|Proceed|Next)", re.I))
            if await btn.count():
                try:
                    await btn.first.scroll_into_view_if_needed()
                except:
                    pass
                try:
                    await btn.first.click(timeout=1000)
                except:
                    try:
                        eh = await btn.first.element_handle()
                        await frame.evaluate(
                            "(el)=>{el && el.removeAttribute && el.removeAttribute('disabled');"
                            "el && el.classList && el.classList.remove('disabled');"
                            "try{el.click()}catch(e){}}", eh
                        )
                    except:
                        pass
                try:
                    await clear_any_alert(frame)
                except:
                    pass
                return True
        except:
            pass

        # CSS 셀렉터 후보
        selectors = [
            "#btnNext", ".btn-next", "button.next", "a.next",
            "button:has-text('다음')", "a:has-text('다음')",
            "button:has-text('다음으로')", "button:has-text('다음 단계')",
            "button:has-text('결제')",
            "[id*='Next' i]", "[class*='next' i]"
        ]
        for sel in selectors:
            try:
                loc = frame.locator(sel)
                if not await loc.count():
                    continue
                el = loc.first
                try:
                    await el.scroll_into_view_if_needed()
                except:
                    pass
                # disabled 우회
                try:
                    eh = await el.element_handle()
                    await frame.evaluate(
                        "(el)=>{el && el.removeAttribute && el.removeAttribute('disabled');"
                        "el && el.classList && el.classList.remove('disabled');}", eh
                    )
                except:
                    pass
                # 클릭 시도
                try:
                    await el.click(timeout=1000)
                except:
                    try:
                        eh = await el.element_handle()
                        await frame.evaluate("(el)=>{try{el.click()}catch(e){}}", eh)
                    except:
                        pass
                try:
                    await clear_any_alert(frame)
                except:
                    pass
                return True
            except:
                pass

        # Enter 키 마지막 시도
        try:
            await frame.keyboard.press("Enter")
            try:
                await clear_any_alert(frame)
            except:
                pass
            return True
        except:
            pass

        return False

    next_ok = await _try_next()

    # (4) 네트워크 힌트(선택): qty 반영되면 종종 아래 api가 연쇄로 나감 (로그 확인용 — 실패해도 무시)
    try:
        await frame.wait_for_timeout(120)
    except:
        pass

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

async def reached_payment(p: Page, timeout_ms: int = PAYMENT_DETECT_TIMEOUT):
    """
    결제 도착을 오탐 없이 감지:
      1) 프레임 URL에 /(payment|order)\b 가 있어야 함
      2) 동시에 현재 페이지가 zone/seat/price 단계가 아니어야 함 (음성 소거)
      3) 결제 관련 DOM 마커(결제하기 버튼, 약관 등) 중 1개 이상 보여야 확정
    """
    RX = re.compile(r"/(payment|order)\b", re.I)

    async def looks_like_payment_dom() -> bool:
        cand = [
            "form#payForm",
            "button#btnPay",
            "button:has-text('결제')",
            "text=결제 수단",
            "text=약관 동의",
            ".payment-method", ".payment-list",
        ]
        try:
            for sel in cand:
                loc = p.locator(sel).first
                if await loc.is_visible(timeout=800):
                    return True
        except:
            pass
        return False

    def scan_frames_once():
        try:
            for f in p.frames:
                u = (getattr(f, "url", "") or "")
                if RX.search(u):
                    return True, u
        except:
            pass
        return False, None

    # 1) 프레임 스캔
    ok, u = scan_frames_once()
    if not ok:
        try:
            # 1회성 이벤트 대기 (짧게)
            with p.expect_event("framenavigated", timeout=timeout_ms) as _:
                pass
        except:
            return False, None
        ok, u = scan_frames_once()

    if not ok:
        return False, None

    # 2) 음성 소거: 존/좌석/가격 단계면 결제 아님
    try:
        if await is_zone_page(p) or await is_seat_page(p) or await is_price_page(p):
            return False, None
    except:
        # 보수적으로 통과시키지 않음
        return False, None

    # 3) 결제 DOM 마커 확인
    if await looks_like_payment_dom():
        return True, u
    return False, None


def _sum_zone(zone_like: dict) -> tuple[int|None, int|None, dict]:
    """
    blockSummary2 또는 baseMap의 zone-like 구조에서 총좌석/잔여/유형별 카운트를 안전하게 합산
    """
    total = 0
    remain = 0
    by = {}

    # blockSummary2 패턴
    # - summary.totalSeatCnt / summary.rmnSeatCnt
    # - blockList[*].rmnSeatCnt, .totSeatCnt
    summ = (zone_like or {}).get("summary") or {}
    t = _to_int(summ.get("totalSeatCnt"))
    r = _to_int(summ.get("rmnSeatCnt"))
    if t is not None: total += t
    if r is not None: remain += r

    for b in (zone_like or {}).get("blockList") or []:
        tt = _to_int(b.get("totSeatCnt"))
        rr = _to_int(b.get("rmnSeatCnt"))
        if tt is not None: total += tt
        if rr is not None: remain += rr

    # baseMap 패턴 (존 목록)
    for z in (zone_like or {}).get("zoneList") or []:
        tt = _to_int(z.get("totalSeatCnt") or z.get("totalCnt"))
        rr = _to_int(z.get("rmnSeatCnt") or z.get("remainCnt"))
        if tt is not None: total += tt
        if rr is not None: remain += rr

    # None 처리: 어느 쪽도 못 구했으면 None로
    if total == 0: total = None
    if remain == 0 and t is None and r is None: remain = None

    return total, remain, by

def _to_int(x):
    if x is None: return None
    if isinstance(x, (int, float)): return int(x)
    try:
        return int(str(x).replace(",", "").strip())
    except Exception:
        return None


def _sum_seatlist(statusList: dict):
    # baseMap.zoneList가 비거나 없는 경우 대비
    d = statusList or {}
    seats = d.get("seatList") or d.get("seats") or []
    total = len(seats)
    def ok(s):
        sale = str(s.get("sale_yn") or s.get("saleYn") or "N").upper() == "Y"
        rsv  = str(s.get("rsv_yn")  or s.get("rsvYn")  or "N").upper() == "Y"
        # 예약 안 걸린 판매 가능 좌석
        return sale and (not rsv)
    remain = sum(1 for s in seats if ok(s))
    return total, remain

def _sum_nrs(blockSummary2: dict):
    d = blockSummary2 or {}
    lists = (d.get("blockList") or d.get("blocks") or d.get("areaList") or [])
    total = remain = 0
    for b in lists:
        total  += _to_int(b.get("total_cnt")  or b.get("totalCnt")  or b.get("plan_cnt")  or 0)
        remain += _to_int(b.get("remain_cnt") or b.get("remainCnt") or b.get("rest_cnt")  or 0)
    sumry = d.get("summary") or {}
    total  = max(total,  _to_int(sumry.get("total")  or sumry.get("totalCnt")  or 0))
    remain = max(remain, _to_int(sumry.get("remain") or sumry.get("remainCnt") or 0))
    return total, remain

def _detect_plan(prodSummary, baseMap, blockSummary2) -> str:
    plan = (
        (prodSummary or {}).get("planType")
        or (baseMap or {}).get("planType")
        or (prodSummary or {}).get("plan")
        or (baseMap or {}).get("plan")
    )
    if plan: return plan.upper()

    # 존/좌석 추정 우선
    if (baseMap and (baseMap.get("zoneList") or baseMap.get("floor") or baseMap.get("floorList"))) \
       or (blockSummary2 and (blockSummary2.get("blockList") or blockSummary2.get("summary"))):
        return "SEAT"   # ← 기존엔 NRS로 오판

    return "NRS"


async def is_true_booking_page(p):
    path = (await p.evaluate("location.pathname")).lower()
    if path != "/booking":
        return False
    # 결제 요소가 실제로 보여야 함 (결제수단/주문요약 등이 렌더된 상태)
    has_payment = await p.locator(
        "#payment, #paymentMethod, [data-testid='payment-methods'], "
        "text=/결제수단|카드결제|무통장입금|주문서/i"
    ).count() > 0
    # 아직 onestop 흐름(존/좌석/수량) 요소가 보이면 결제 아님
    still_rs = await p.locator(
        ".block-summary, [data-zone], text=/존\\s*선택/i, "
        "[name='quantity'], [data-testid='fcfs-qty']"
    ).count() > 0
    return has_payment and not still_rs

async def detect_phase(p):
    if await is_true_booking_page(p):
        return "결제창"
    try:
        path = await p.evaluate("location.pathname") or ""
    except Exception:
        path = ""
    if "/rs/seat" in path:
        return "좌석선택"
    if "/rs" in path:
        try:
            hasSeatOrZone = await p.evaluate(
                "!!(document.querySelector('#seatMap, .seat-map, #zoneList, .zone-list, .seat-wrap, .zone-wrap'))"
            )
            if hasSeatOrZone:
                return "좌석/존"
        except Exception:
            pass
        return "진행"
    return "진행"

async def force_snapshot_and_hold(p, sdCode: str, *, quiet=False):
    """
    iframe/팝업 없이도 한 방에:
      - filmapi로 prodSeq/sdSeq 매핑
      - rs 세션 워밍업 + 필요한 API 동시요청
      - 총좌석/잔여 계산 (SEAT = BaseMap.zoneList, NRS = blockSummary2)
      - 좌석 1매 선점(가능 시) 또는 수량 1로 Next 진행
    """
    # 0) prodSeq/sdSeq 매핑
    sched = await get_meta_from_filmapi(p, sdCode)
    if not sched:
        raise RuntimeError(f"[{sdCode}] filmapi sched 없음")
    prodSeq = str(sched.get("prodSeq") or sched.get("prodseq") or "")
    sdSeq   = str(sched.get("sdSeq") or sched.get("sdseq") or "")
    if not (prodSeq and sdSeq):
        raise RuntimeError(f"[{sdCode}] prodSeq/sdSeq 파싱 실패")

    refs = build_onestop_referers({}, prodSeq, sdSeq)

    # 1) 워밍업 (세션 시드)
    try:
        # prodSeq, sdSeq 변수가 이 스코프에 있다면 넘겨주세요(Referer 정확도↑).
        # 없으면 None으로 둬도 동작은 합니다.
        csrfToken, H_RS = await ensure_csrf_and_headers(p, prodSeq=prodSeq if 'prodSeq' in locals() else None,
                                                        sdSeq=sdSeq   if 'sdSeq'   in locals() else None)
        await post_api(p, "/rs/prod",
                    form={"langCd": "ko", "csrfToken": csrfToken or ""},
                    extra_headers=H_RS)
    except Exception:
        if not quiet:
            slog(f"[{sdCode}] rs/prod 워밍업 실패 (무시)")


    # 2) 동시에 긁어오기
    async def _safe(call, *a, **kw):
        try:
            return await call(*a, **kw)
        except Exception as e:
            return {"__error__": str(e)}

    rs_prodChk       = _safe(post_api, "/rs/prodChk",       p, form={}, headers={"Referer": refs["rs"]})
    rs_chkProdSdSeq  = _safe(post_api, "/rs/chkProdSdSeq",  p, form={}, headers={"Referer": refs["rs"]})
    rs_informLimit   = _safe(post_api, "/rs/informLimit",   p, form={}, headers={"Referer": refs["rs"]})
    rs_prodSummary   = _safe(post_api, "/rs/prodSummary",   p, form={}, headers={"Referer": refs["rs"]})
    rs_blockSummary2 = _safe(post_api, "/rs/blockSummary2", p, form={}, headers={"Referer": refs["rs"]})

    seat_baseMap     = _safe(post_api, "/seat/GetRsSeatBaseMap",    p, form={}, headers={"Referer": refs["seat"]})
    seat_statusList  = _safe(post_api, "/seat/GetRsSeatStatusList", p, form={}, headers={"Referer": refs["seat"]})
    rs_ticketType    = _safe(post_api, "/api/v1/rs/tickettype",     p, form={}, headers={"Referer": refs["rs"]})

    (
        prodChk, chkProdSdSeq, informLimit, prodSummary, blockSummary2,
        baseMap, statusList, ticketType
    ) = await asyncio.gather(
        rs_prodChk, rs_chkProdSdSeq, rs_informLimit, rs_prodSummary, rs_blockSummary2,
        seat_baseMap, seat_statusList, rs_ticketType
    )

    # 3) plan 판별
    plan = _detect_plan(prodSummary, baseMap, blockSummary2)

    # 3-1) 총/잔여 계산 (SEAT/NRS/ALL 모두 대응)
    by = {}
    if plan == "SEAT":
        z_total, z_remain = _sum_zone(baseMap)
        # zoneList가 없거나 remain이 0인데 실제론 좌석 리스트가 올 때 대비
        if (z_total == 0) and (statusList and isinstance(statusList, dict)):
            z_total2, z_remain2 = _sum_seatlist(statusList)
            z_total = max(z_total, z_total2)
            z_remain = max(z_remain, z_remain2)
        total, remain = z_total, z_remain
        by = {"SEAT": remain}

    elif plan == "NRS":
        n_total, n_remain = _sum_nrs(blockSummary2)
        # 일부 회차는 blockSummary2가 빈 경우가 있어서 prodSummary 같은 데서 보강할 수 있으면 보강해도 됨
        total, remain = n_total, n_remain
        by = {"NRS": remain}

    elif plan == "ALL":
        # 하이브리드: SEAT + NRS 합산
        z_total, z_remain = _sum_zone(baseMap)
        if z_total == 0 and statusList:
            z_total2, z_remain2 = _sum_seatlist(statusList)
            z_total = max(z_total, z_total2)
            z_remain = max(z_remain, z_remain2)
        n_total, n_remain = _sum_nrs(blockSummary2)
        total  = (z_total or 0) + (n_total or 0)
        remain = (z_remain or 0) + (n_remain or 0)
        by = {"SEAT/ZONE": z_remain, "NRS": n_remain}

    else:
        # 모호하면 각 소스에서 최대치/합 적당히 택1 (표시용)
        z_total, z_remain = _sum_zone(baseMap)
        if z_total == 0 and statusList:
            z_total2, z_remain2 = _sum_seatlist(statusList)
            z_total = max(z_total, z_total2)
            z_remain = max(z_remain, z_remain2)
        n_total, n_remain = _sum_nrs(blockSummary2)
        # 총좌석은 있으면 합, 잔여는 보수적으로 max
        total  = (z_total or 0) + (n_total or 0)
        remain = max(z_remain, n_remain)
        plan   = plan or "UNKNOWN"
        by = {"SEAT/ZONE": z_remain, "NRS": n_remain}

    log_ok(f"[SEAT] sdCode={sdCode} plan={plan} total={total} remain={remain} by={by}")

    # 4) 이동 로직
    moved = False
    if plan == "NRS" and remain > 0:
        # 선착순은 수량 1 → Next
        try:
            moved = await ensure_qty_one_and_next(p, referer=refs["rs"])
        except Exception as e:
            slog(f"[{sdCode}] NRS next 실패: {e}")

    elif plan == "SEAT" and remain > 0:
        # 지정석은 좌석 선점 후 Next
        try:
            picked = await pick_seat_via_api(p, prodSeq, sdSeq, qty=1)
            if picked:
                moved = await ensure_qty_one_and_next(p, referer=refs["seat"])
        except Exception as e:
            slog(f"[{sdCode}] SEAT 좌석선점 실패: {e}")

    elif plan == "ALL":
        # ❗중요: ALL을 NRS처럼 바로 Next 누르면 '결제창 오인' 난다.
        # 여기서는 정보만 모으고, 블록/존 진입-선택은 별도 루틴으로 하거나
        # remain>0이면 UI/API로 블록을 먼저 잡아야 함.
        pass

    # 5) 현재 단계 로깅 (DOM 기반 판정)
    phase = await detect_phase(p)
    log_ok(f"[{sdCode}] {phase} 페이지: {p.url}")

    return {
        "sdCode": sdCode, "prodSeq": prodSeq, "sdSeq": sdSeq,
        "plan": plan, "total": total, "remain": remain, "moved": moved
    }



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
        ok, final_url = await reached_payment(work)  # (bool, url)
        if ok:
            log_ok(f"✅ 결제창 진입: {final_url or work.url}")
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
            dlog("[STEP] Seat page → API-가이드 좌석 선택 후 Next")
            # prod/sd가 있으면 API로 '좋은' 좌석을 먼저 고른 뒤 DOM에서 그 좌석을 클릭
            picked = False
            try:
                ps = str((params or {}).get("prodSeq",""))
                ss = str((params or {}).get("sdSeq","") or sd_code)
                if ps and ss:
                    picked = await force_pick_one_seat(scope, ps, ss)
            except Exception as e:
                dlog(f"[STEP] force_pick_one_seat warn: {e}")
            if not picked:
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

    if ok:
        log("✅ 로그인 감지. 동시 예매 시작.")
        # ⬇️ 로그인 세션이 살아있는 같은 Page로 스냅샷/선점 러너 실행
        try:
            await run_auto_snapshots(p, do_hold=True)  # <- 여기!
        except Exception as e:
            wlog(f"[SNAP] auto snapshot 실패: {e}")

    await p.close()
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
# --- compat logging shims (place after ilog/dlog/wlog/elog) ---
def log_ok(*a, **kw):
    return dlog(*a, **kw)

def slog(*a, **kw):
    return wlog(*a, **kw)

# ── ADD NEW FUNCTION (place near ensure_qty_one/ _click_next_in_frame) ───────
async def ensure_qty_one_and_next(scope):
    """
    현재 스코프(페이지/프레임)에서 수량 1 세팅 후 Next 성격 버튼 클릭.
    성공하면 True, 아니면 False
    """
    # 예약 프레임 우선 스코프 보정
    try:
        p = getattr(scope, "page", None) or scope
        scope = find_booking_scope(p) or scope
    except:
        pass

    try:
        ok = await ensure_qty_one(scope)
    except Exception:
        ok = False

    moved = False
    try:
        # 폭넓게 Next 류 시도
        if await click_like(scope, RX_NEXT):
            moved = True
            try:
                await clear_any_alert(scope)
            except:
                pass
        else:
            # 프레임 전용 보강
            try:
                await _click_next_in_frame(scope)
                moved = True
                try:
                    await clear_any_alert(scope)
                except:
                    pass
            except Exception:
                pass
    except Exception:
        pass
    return bool(ok and moved)
# ──────────────────────────────────────────────────────────────────────────────

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
    await arm_payment_hold(page)
    try:
        # 작품 페이지 → 예매창
        res_url = BASE_RESMAIN.format(sd=sd)
        await page.goto(res_url, wait_until="domcontentloaded", timeout=OPEN_TIMEOUT)
        title = (await find_title(page)) or f"sdCode {sd}"
        log(f"🎬 [{sd}] {title}")
        # ── NEW: API 스냅샷 선행 (매진이어도 총/잔여 산출) ─────────────────────
        try:
            snap = await force_snapshot_and_hold(page, sd)
            # 참고: snap["moved"] 가 True 여도, plan=ALL(존좌석)이면 결제창이 아닐 수 있음.
            # 결제창은 location.pathname 에 /booking 포함될 때만 진입으로 간주.
        except Exception as e:
            wlog(f"[{sd}] snapshot 실패: {e}")
        # ─────────────────────────────────────────────────────────────────────────

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

        # ❶ sdCode → (prodSeq, sdSeq) 항상 먼저 강제
        if not params.get("prodSeq") or not params.get("sdSeq"):
            try:
                p2, s2 = await map_sd_from_filmapi(scope_onestop, params.get("sdCode") or sd)
                if p2 and s2:
                    params["prodSeq"], params["sdSeq"] = str(p2), str(s2)
            except Exception as e:
                dlog(f"[force map sdCode] {e}")

        # ❷ 그 다음에 RS 필수 보강
        pack = await ensure_full_rs_params(scope_onestop, params.get("prodSeq"), params.get("sdSeq"))
        # === RS 파라미터가 비면 booking DOM에서 마지막으로 강제 하베스트 ===
        if not params.get("prodSeq") or not params.get("sdSeq"):
            try:
                ctx_in = await _harvest_booking_ctx(scope_onestop)
                if ctx_in:
                    if not params.get("prodSeq"):   params["prodSeq"]   = ctx_in.get("prodSeq") or ""
                    if not params.get("sdSeq"):     params["sdSeq"]     = ctx_in.get("sdSeq")   or ""
                    if not params.get("chnlCd"):    params["chnlCd"]    = ctx_in.get("chnlCd") or "WEB"
                    if not params.get("csrfToken"): params["csrfToken"] = ctx_in.get("csrf")    or params.get("csrfToken","")
            except Exception as e:
                dlog(f"[COLLECT] harvest_booking_ctx warn: {e}")
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
        # ▶ 메타 + 총/잔여 1줄 로그
        info = await collect_show_info(scope0 or work, params["prodSeq"], params["sdSeq"], sdCode=sd)
        log_show_line(sd, info)
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
            # 👇 결제/가격 단계 진입 직후, 창 유지
            try:
                global PAYMENT_DETECTED; PAYMENT_DETECTED = True
            except: 
                pass
            await hold_at_payment(scope.page if hasattr(scope, "page") else scope)
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
        await install_cors_demo(ctx)
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
            KEEP_OPEN_ON_SUCCESS = (os.getenv("KEEP_OPEN_ON_SUCCESS", "1") == "1")
            keep = (KEEP_OPEN_ON_SUCCESS and any(r.ok for r in results)) \
                or (os.getenv("PAY_STAY","0")=="1" and KEEP_BROWSER_ON_HOLD) \
                or PAYMENT_DETECTED
            # ✅ 창 유지: hold 플래그가 붙은 페이지가 하나라도 있으면 닫힐 때까지 대기
            held_pages = [p for p in ctx.pages if getattr(p, "_hold_open", False)]
            if keep and held_pages:
                print("[HOLD] 하나 이상의 창이 열려 있습니다. 결제/예약을 마치고 창을 닫으면 종료됩니다. (Ctrl+C로 즉시 종료)")
                try:
                    await asyncio.gather(*(p.wait_for_event("close") for p in held_pages))
                except Exception:
                    pass
                # 모두 닫힌 뒤 정리하고 리턴
                await ctx.close(); await browser.close()
                return results
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
                    if st in ("Y","AVAILABLE","OK","SS01000"): avail += 1
                    if d.get("disabled") is False: avail += 1
            if sd and total:
                o = out.setdefault(sd, {"total":0,"remain":0})
                o["total"] = max(o["total"], total)
                o["remain"] = max(o["remain"], avail)
    # 저장
    pathlib.Path(out_json).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[HAR] saved → {out_json}")
    return out

# bf.py — BIFF 자동예매/좌석 파싱 (최종본, URL 비의존 단계판별)
#  - 로그인: https://biff.maketicket.co.kr/ko/mypageLogin (자동 입력/제출, iframe 대응)
#  - FilmAPI(prodList) GET→POST 폴백 + Ajax 헤더(+정상 Referer)
#  - CSRF 강제 확보(메타/쿠키/헤더 프로빙)
#  - RS 프라이밍(prod→prodChk→chkProdSdSeq→informLimit→prodSummary)
#  - 좌석 API: x-www-form-urlencoded + Ajax 헤더
#  - "결제창 진입" 판정: URL이 아니라 DOM/PG iframe/결제UI 신호로 판별
#  - 모든 실행 아티팩트 저장(runs/<timestamp>/bf.log, events.ndjson, network.har, trace.zip)

import asyncio, contextlib, json, os, sys, time, re
from typing import Optional, Any, Dict, Tuple, List
from dataclasses import dataclass
from urllib.parse import urlencode, unquote
from datetime import datetime
from pathlib import Path
import logging

from playwright.async_api import async_playwright, BrowserContext, Page, Frame, TimeoutError as PWTimeout

# ===== 설정 =====
HEADLESS         = True
LANG             = "KO"
CHNL             = "WEB"
REQ_TIMEOUT_MS   = 15000
STEP_TIMEOUT_MS  = 20000
RETRY_COUNT      = 1
CONCURRENCY      = 4

# --show / --headful 플래그로 창 띄우기
if "--show" in sys.argv or "--headful" in sys.argv:
    HEADLESS = False
    # 플래그는 argv에서 제거 (sdCode 파싱에 영향 없게)
    sys.argv = [a for a in sys.argv if a not in ("--show", "--headful")]

# ===== 도메인 =====
PORTAL_SITE  = "https://biff.maketicket.co.kr"
PORTAL_LOGIN = f"{PORTAL_SITE}/ko/mypageLogin"
RESMAIN_TPL  = f"{PORTAL_SITE}/ko/resMain?sdCode={{sd}}"   # FilmAPI referer용

ONESTOP_SITE = "https://filmonestop.maketicket.co.kr"
API          = "https://filmonestopapi.maketicket.co.kr"
FILMAPI      = "https://filmapi.maketicket.co.kr"

SSO_LOCK = asyncio.Lock()
# ===== 로그인 정보 (요청대로 고정; 로그엔 마스킹) =====
LOGIN_ID_DEFAULT = "01036520261"
LOGIN_PW_DEFAULT = "040435"
LOGIN_ID = os.getenv("BF_ID", LOGIN_ID_DEFAULT)
LOGIN_PW = os.getenv("BF_PW", LOGIN_PW_DEFAULT)

# ===== 실행 아티팩트 =====
RUN_ID  = datetime.now().strftime("%Y%m%d-%H%M%S")
RUN_DIR = Path.cwd() / "runs" / RUN_ID
RUN_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH   = RUN_DIR / "bf.log"
JSONL_PATH = RUN_DIR / "events.ndjson"
HAR_PATH   = RUN_DIR / "network.har"
TRACE_PATH = RUN_DIR / "trace.zip"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)

def _write_event(ev: dict):
    try:
        with open(JSONL_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    except Exception:
        pass

def log_info(msg: str): logging.info(msg); _write_event({"t":"info","msg":msg})
def log_err(msg: str):  logging.error(msg); _write_event({"t":"error","msg":msg})

# ===== 데이터 모델 =====
@dataclass
class Show:
    sdCode: str
    prodSeq: int
    sdSeq: int
    perfDate: str
    title: str
    venue: str
    hall: str

@dataclass
class Tokens:
    rs_csrf: str = ""
    seat_csrf: str = ""
    referer_rs: str = ""
    referer_seat: str = ""

# ===== 유틸 =====

# 상단에 STRICT 플래그
STRICT_SSO = int(os.getenv("STRICT_SSO", "1"))

def _assert_sso_ready(page: "Page", url: str = ""):
    if not STRICT_SSO:
        return
    if getattr(page, "_sso_ready", False):
        return
    if url and (url.startswith(ONESTOP_SITE) or url.startswith(API)):
        raise RuntimeError(f"STRICT_SSO: onestop/api 접근을 bridge_sso() 전에 시도: {url}")


def ok_text(s: Optional[str]) -> str:
    return (s or "").strip()

def compact_date_from_en(s: str) -> str:
    m = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", s)
    return f"{m.group(1)}{m.group(2)}{m.group(3)}" if m else ""

async def get_header(resp, name: str) -> str:
    try:
        h = await resp.headers()
        for k, v in h.items():
            if k.lower() == name.lower():
                return v
    except Exception:
        with contextlib.suppress(Exception):
            h2 = resp.headers
            if isinstance(h2, dict):
                for k, v in h2.items():
                    if k.lower() == name.lower():
                        return v
    return ""

async def _save_snap(page: Page, tag: str):
    with contextlib.suppress(Exception):
        p = RUN_DIR / f"{tag}.png"
        await page.screenshot(path=str(p), full_page=True)

# ===== Ajax POST 래퍼 (x-www-form-urlencoded 강제) =====
async def request_json(ctx: BrowserContext, method: str, url: str, *,
                       data: Optional[dict] = None,
                       headers: Optional[dict] = None,
                       timeout_ms: int = REQ_TIMEOUT_MS,
                       retry: int = RETRY_COUNT) -> Any:
    base_headers = {
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    }
    if headers:
        base_headers.update(headers)

    last_exc = None
    method = method.upper()
    for attempt in range(retry + 1):
        try:
            if method == "POST":
                body = ""
                if isinstance(data, dict):
                    body = urlencode(data)
                    base_headers.setdefault("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                elif isinstance(data, str):
                    body = data
                    base_headers.setdefault("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                resp = await ctx.request.post(url, headers=base_headers, data=body, timeout=timeout_ms)
            else:
                resp = await ctx.request.get(url, headers=base_headers, timeout=timeout_ms)

            if resp.status != 200:
                txt = await resp.text()
                _write_event({"t":"http","ok":False,"method":method,"url":url,"status":resp.status,
                              "headers":base_headers, "data":data or {}, "body":txt[:800]})
                raise RuntimeError(f"HTTP {resp.status} for {url}")

            try:
                js = await resp.json()
                _write_event({"t":"http","ok":True,"method":method,"url":url,"status":resp.status})
                return js
            except Exception:
                txt = await resp.text()
                _write_event({"t":"http","ok":False,"method":method,"url":url,"status":resp.status,
                              "reason":"Non-JSON","body":txt[:800]})
                raise RuntimeError(f"Non-JSON for {url}: {txt[:300]}...")
        except Exception as e:
            last_exc = e
            await asyncio.sleep(0.5 * (attempt + 1))
    raise last_exc if last_exc else RuntimeError(f"request_json failed: {url}")

# ===== 헤더 빌더 =====
def headers_rs(tok: Tokens) -> Dict[str, str]:
    if not tok.referer_rs:
        tok.referer_rs = f"{ONESTOP_SITE}/ko/onestop/rs"
    h = {
        "Origin": ONESTOP_SITE,
        "Referer": tok.referer_rs,
    }
    if tok.rs_csrf:
        h["X-CSRF-TOKEN"] = tok.rs_csrf
    return h

def headers_seat(tok: Tokens, show: Show) -> Dict[str, str]:
    if not tok.referer_seat:
        tok.referer_seat = f"{ONESTOP_SITE}/ko/onestop/rs/seat?prodSeq={show.prodSeq}&sdSeq={show.sdSeq}"
    h = {
        "Origin": ONESTOP_SITE,
        "Referer": tok.referer_seat,
    }
    if tok.seat_csrf:
        h["X-CSRF-TOKEN"] = tok.seat_csrf
    return h

# ==== Storage helpers: sessionStorage dump/prime =================================
def _ss_to_jsobj(obj: dict) -> str:
    # 안전하게 JS 코드로 인라인
    import json
    return json.dumps({k: (v if v is not None else "") for k, v in obj.items()}, ensure_ascii=False)

async def dump_session_storage(page: Page) -> dict:
    """현재 탭의 sessionStorage 전체를 dict로 덤프"""
    try:
        return await page.evaluate("""() => {
            const out = {};
            for (let i=0;i<sessionStorage.length;i++){
              const k = sessionStorage.key(i);
              out[k] = sessionStorage.getItem(k);
            }
            return out;
        }""")
    except Exception:
        return {}

async def prime_session_storage(page: Page, origin: str, ss: dict):
    """해당 origin에 로드되기 *전*에 sessionStorage를 주입 (탭 전역 init-script)"""
    if not ss: 
        return
    js = f"""
    (() => {{
      const ORIGIN = {repr(origin)};
      const SEED = {_ss_to_jsobj(ss)};
      // init-script는 모든 문서에서 실행되므로 origin 체크
      if (location.origin === ORIGIN) {{
        try {{
          for (const [k,v] of Object.entries(SEED)) {{
            try {{ sessionStorage.setItem(k, v); }} catch (_e) {{}}
          }}
          // 디버깅 힌트
          // console.debug('[INIT] sessionStorage primed for', ORIGIN, Object.keys(SEED).length);
        }} catch (_e) {{}}
      }}
    }})();
    """
    await page.add_init_script(js)


# ===== 로그인 판정/자동화 =====
async def _is_logged_in(page: Page) -> bool:
    try:
        html = (await page.content()).lower()
        if await page.locator("a:has-text('로그아웃'), button:has-text('로그아웃'), a[href*='logout']").count() > 0:
            return True
        if "마이페이지" in html or "mypage" in html:
            return True
        return False
    except Exception:
        return False

async def _has_login_form(ctx) -> bool:
    try:
        return await ctx.locator('input[type="password"]').count() > 0
    except Exception:
        return False

async def _find_login_context(page: Page) -> Tuple[Optional[Page], Optional[Frame]]:
    if await page.locator('input[type="password"]').count():
        return page, None
    for fr in page.frames:
        with contextlib.suppress(Exception):
            if await fr.locator('input[type="password"]').count():
                return None, fr
    return page, None

async def auto_login(page: Page) -> None:
    ctx_page, ctx_frame = await _find_login_context(page)
    ctx = ctx_frame if ctx_frame else page

    id_candidates = [
        'input[name="loginId"]','input[name="userId"]','input[id*="login"]',
        'input[id*="user"]','input[name*="id"]','input[type="tel"]','input[type="text"]'
    ]
    pw_candidates = [
        'input[name="loginPw"]','input[name="userPw"]','input[id*="pw"]',
        'input[name*="pw"]','input[type="password"]'
    ]
    btn_candidates = [
        "button[type=submit]","button:has-text('로그인')","input[type=submit]",
        "a:has-text('로그인')","#btnLogin",".btnLogin"
    ]

    id_box = pw_box = None
    for sel in id_candidates:
        loc = ctx.locator(sel).first
        if await loc.count(): id_box = loc; break
    for sel in pw_candidates:
        loc = ctx.locator(sel).first
        if await loc.count(): pw_box = loc; break
    if id_box: await id_box.fill(LOGIN_ID)
    if pw_box: await pw_box.fill(LOGIN_PW)

    masked = LOGIN_ID[:3] + "*"*(len(LOGIN_ID)-5) + LOGIN_ID[-2:]
    log_info(f"→ 로그인 시도: id={masked}, pw=********")

    clicked = False
    for sel in btn_candidates:
        loc = ctx.locator(sel).first
        if await loc.count():
            with contextlib.suppress(Exception):
                await loc.click(timeout=4000)
                clicked = True
                break
    if not clicked and pw_box:
        with contextlib.suppress(Exception):
            await pw_box.press("Enter")

    with contextlib.suppress(Exception):
        await page.wait_for_load_state("networkidle", timeout=6000)

async def wait_for_login(ctx: BrowserContext, page: Page) -> None:
    # 1) 고정 로그인 링크로 이동
    await page.goto(PORTAL_LOGIN, wait_until="domcontentloaded", timeout=STEP_TIMEOUT_MS)
    await auto_login(page)

    log_info("🔐 [포털(biff)] 로그인 화면입니다. 로그인만 완료하세요…")
    deadline = time.time() + 180
    while time.time() < deadline:
        if await _is_logged_in(page):
            log_info("✅ [포털(biff)] 로그인 감지.")
            break
        if await _has_login_form(page):
            await auto_login(page)
        await asyncio.sleep(1.0)

    if not await _is_logged_in(page):
        raise RuntimeError("로그인 감지 실패")

    # onestop 쪽은 각 상영건 rs/seat 페이지 진입 시에 SSO/CSRF가 붙는 경우가 많음.
    # 여기서 굳이 로그인 여부를 강제 확인하지 않고, per-show 워밍업(warmup_rs/seat)에서 처리.
    log_info("✅ 로그인/세션 준비 완료.")


# ==== SSO bridge: portal(resMain) -> onestop(rs) ====
async def bridge_sso(page: Page, show: Show) -> Page:
    res_url = RESMAIN_TPL.format(sd=show.sdCode)
    await page.goto(res_url, wait_until="domcontentloaded", timeout=STEP_TIMEOUT_MS)

    # 기존 selector 로직 그대로 유지
    sel_exact = f"a[href*='filmonestop.maketicket.co.kr'][href*='prodSeq={show.prodSeq}']"
    sel_onestop = f"a[href*='/ko/onestop/rs'][href*='prodSeq={show.prodSeq}']"
    link = page.locator(sel_exact).first
    if not await link.count():
        link = page.locator(sel_onestop).first
    if not await link.count():
        for s in ["a:has-text('바로 예매')", "button:has-text('바로 예매')",
                  "a:has-text('예매')", "button:has-text('예매')"]:
            loc = page.locator(s).first
            if await loc.count():
                link = loc
                break
    if not await link.count():
        raise RuntimeError(f"[SSO] 예매 링크를 찾지 못함: {res_url} (STRICT_SSO)")

    popup = None
    try:
        # ★ 팝업 우선: 새 창이 뜨는 경우를 확실히 잡는다
        async with page.expect_popup() as pinfo:
            await link.click()
        popup = await pinfo.value
        await popup.wait_for_load_state("domcontentloaded", timeout=STEP_TIMEOUT_MS)
    except Exception:
        # ★ 동일 탭 폴백
        await link.click()
        await page.wait_for_url(r"https://filmonestop\.maketicket\.co\.kr/.*", timeout=STEP_TIMEOUT_MS)
        with contextlib.suppress(Exception):
            await page.wait_for_load_state("networkidle", timeout=6000)

    # ★ 팝업으로 열렸다면: onestop 세션/스토리지를 캐시해서 작업 탭(page)에 주입
    if popup:
        try:
            ss = await dump_session_storage(popup)
            await prime_session_storage(page, ONESTOP_SITE, ss)
        finally:
            with contextlib.suppress(Exception):
                await popup.close()

    # (선택) 디버그: onestop 쿠키 이름 찍기
    with contextlib.suppress(Exception):
        cs = await page.context.cookies(ONESTOP_SITE)
        names = ", ".join(sorted({c.get("name","") for c in cs if c.get("name")}))
        log_info(f"[SSO] onestop 쿠키: {names}")


# ===== FilmAPI (prodList) =====
async def filmapi_get_show(ctx: BrowserContext, sdCode: str) -> Show:
    url = f"{FILMAPI}/api/v1/prodList?sdCode={sdCode}"
    headers = {
        "Referer": RESMAIN_TPL.format(sd=sdCode),
        "Origin": PORTAL_SITE,
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        resp = await ctx.request.get(url, headers=headers, timeout=REQ_TIMEOUT_MS)
        if resp.status == 200:
            js = await resp.json()
        else:
            raise RuntimeError(f"HTTP {resp.status} for GET {url}")
    except Exception as e_get:
        log_err(f"[FilmAPI][GET] 실패: {e_get} → POST 폴백")
        resp = await ctx.request.post(url, data="", headers=headers, timeout=REQ_TIMEOUT_MS)
        if resp.status != 200:
            txt = await resp.text()
            _write_event({"t":"filmapi","method":"POST","url":url,"status":resp.status,"body":txt[:800]})
            raise RuntimeError(f"HTTP {resp.status} for POST {url}")
        js = await resp.json()

    item = (js.get("prodList") or [{}])[0]
    prodSeq  = int(item["prodSeq"])
    sdSeq    = int(item["sdSeq"])
    perfDate = compact_date_from_en(item.get("sdDateEn") or item.get("sdDate") or "")
    return Show(
        sdCode=str(sdCode),
        prodSeq=prodSeq,
        sdSeq=sdSeq,
        perfDate=perfDate,
        title=ok_text(item.get("perfMainNm")),
        venue=ok_text(item.get("venueNm")),
        hall=ok_text(item.get("hallNm")),
    )

# ===== RS/SEAT 워밍 + CSRF =====
async def warmup_rs(page, show, *args, **kw):
    _assert_sso_ready(page, f"{ONESTOP_SITE}/ko/onestop/rs?prodSeq={show.prodSeq}&sdSeq={show.sdSeq}")
    tok = Tokens()
    try:
        url = f"{ONESTOP_SITE}/ko/onestop/rs?prodSeq={show.prodSeq}&sdSeq={show.sdSeq}"
        await page.goto(url, wait_until="domcontentloaded", timeout=STEP_TIMEOUT_MS)
        tok.referer_rs = url
        token = await page.evaluate("""() => {
            const pick = (s) => document.querySelector(s)?.content || document.querySelector(s)?.value || '';
            return pick('meta[name="csrf-token"]') || pick('meta[name=_csrf]') || pick('#csrfToken') || '';
        }""")
        tok.rs_csrf = (token or "").strip()
    except Exception:
        pass
    return tok

async def warmup_seat(page: Page, show: Show, tok: Tokens) -> Tokens:
    try:
        url = f"{ONESTOP_SITE}/ko/onestop/rs/seat?prodSeq={show.prodSeq}&sdSeq={show.sdSeq}"
        await page.goto(url, wait_until="domcontentloaded", timeout=STEP_TIMEOUT_MS)
        tok.referer_seat = url
        if not tok.seat_csrf:
            token = await page.evaluate("""() => {
                const pick = (s) => document.querySelector(s)?.content || document.querySelector(s)?.value || '';
                return pick('meta[name="csrf-token"]') || pick('meta[name=_csrf]') || pick('#csrfToken') || '';
            }""")
            tok.seat_csrf = (token or "").strip()
    except Exception:
        pass
    return tok

async def ensure_csrf_token(page: Page, show) -> str:
    rs_url = f"{ONESTOP_SITE}/ko/onestop/rs?prodSeq={show.prodSeq}&sdSeq={show.sdSeq}"
    with contextlib.suppress(Exception):
        await page.goto(rs_url, wait_until="domcontentloaded", timeout=STEP_TIMEOUT_MS)
        with contextlib.suppress(Exception):
            await page.wait_for_load_state("networkidle", timeout=4000)

    # (A) DOM/meta/히든/전역변수/쿠키 mirror
    try:
        token = await page.evaluate("""() => {
          const pick = (s) => document.querySelector(s)?.content || document.querySelector(s)?.value || '';
          const v = pick('meta[name="csrf-token"]') || pick('meta[name=_csrf]') ||
                    pick('#csrfToken') || pick('input[name=_csrf]') || '';
          if (v && v.length > 8) return v;
          for (const k of ['csrfToken','_csrf','csrf']) {
            if (window[k] && String(window[k]).length > 8) return String(window[k]);
          }
          const m = (document.cookie || '').match(/XSRF-TOKEN=([^;]+)/i);
          if (m && decodeURIComponent(m[1]).length > 8) return decodeURIComponent(m[1]);
          return '';
        }""")
        if token and len(token) > 8:
            return token.strip()
    except Exception:
        pass

    # (B) /rs/prod POST 후 헤더/Set-Cookie에서 줍기 (perfDate 필수!)
    form = (
        f"langCd={LANG}&chnlCd={CHNL}"
        f"&prodSeq={show.prodSeq}&sdSeq={show.sdSeq}"
        f"&perfDate={show.perfDate}"
    )
    resp = await page.context.request.post(
        f"{API}/rs/prod",
        headers={
            "X-Requested-With": "XMLHttpRequest",
            "Origin": ONESTOP_SITE,
            "Referer": rs_url,
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        },
        data=form,
        timeout=REQ_TIMEOUT_MS,
    )

    try:
        for k, v in (await resp.headers()).items():
            if k.lower() in ("x-csrf-token", "csrf-token") and v and len(v) > 8:
                return v.strip()
    except Exception:
        pass

    try:
        sc = (await resp.headers()).get("set-cookie") or ""
        m = re.search(r"XSRF-TOKEN=([^;]+)", sc, re.I)
        if m:
            from urllib.parse import unquote
            val = unquote(m.group(1))
            if len(val) > 8:
                return val.strip()
    except Exception:
        pass

    # (C) 컨텍스트 쿠키 스캔
    try:
        for host in (ONESTOP_SITE, API):
            for c in await page.context.cookies(host):
                nm = (c.get("name") or "").upper()
                if nm in ("XSRF-TOKEN","CSRF-TOKEN","X_CSRF_TOKEN"):
                    from urllib.parse import unquote
                    val = unquote(c.get("value") or "")
                    if len(val) > 8:
                        return val.strip()
    except Exception:
        pass

    return ""



# ===== RS 프라이밍 =====
async def prime_rs(ctx: BrowserContext, show: Show, tok: Tokens) -> None:
    if not tok.rs_csrf:
        raise RuntimeError("missing CSRF: rs_csrf")
    base = {
        "langCd": LANG,
        "csrfToken": tok.rs_csrf,
        "prodSeq": str(show.prodSeq),
        "sdSeq": str(show.sdSeq),
        "chnlCd": CHNL,
        "perfDate": show.perfDate,   # ★ 추가
        "perfDe": show.perfDate,     # ★ 호환 필드(무시되면 OK)
    }
    for path in ("/rs/prod", "/rs/prodChk", "/rs/chkProdSdSeq", "/rs/informLimit", "/rs/prodSummary"):
        await request_json(ctx, "POST", f"{API}{path}", data=base, headers=headers_rs(tok))
# ===== 좌석 통계 =====
async def seat_stats(ctx: BrowserContext, show: Show, tok: Tokens) -> Tuple[str, int, int, Dict[str,int]]:
    form = {
        "langCd": LANG, "csrfToken": tok.rs_csrf,
        "prodSeq": str(show.prodSeq), "sdSeq": str(show.sdSeq),
        "chnlCd": CHNL,
        "perfDate": show.perfDate,    # ★ 추가
        "perfDe": show.perfDate,      # ★ 호환
    }
    bs = await request_json(ctx, "POST", f"{API}/rs/blockSummary2", data=form, headers=headers_rs(tok))
    plan = ok_text(bs.get("seatPlanTycd") or bs.get("plan") or "NRS")
    total = int(bs.get("totalCnt") or 0)
    remain = int(bs.get("remainCnt") or 0)
    bycat = {}
    try:
        for it in (bs.get("list") or []):
            nm = ok_text(it.get("saleTyNm") or it.get("name") or "NRS")
            bycat[nm] = int(it.get("remainCnt") or it.get("qty") or 0)
    except Exception:
        pass

    if plan != "NRS":
        base = await request_json(ctx, "POST", f"{API}/seat/GetRsSeatBaseMap", data=form, headers=headers_seat(tok, show))
        stat = await request_json(ctx, "POST", f"{API}/seat/GetRsSeatStatusList", data=form, headers=headers_seat(tok, show))
        seats = stat.get("seatList") or stat.get("list") or []
        total = len(seats)
        remain = sum(1 for s in seats if str(s.get("sts") or s.get("status") or "").upper() in ("A","AVL","0","FREE","Y"))
    return (plan, total, remain, bycat)

# ===== 단계 판별 (URL 의존 X) =====
async def detect_stage(page: Page) -> str:
    """
    좌석선택/자유석수량/체크아웃/PG-iframe 등 단계 판별.
    반환: 'SEAT' | 'NRS_QTY' | 'CHECKOUT' | 'PG' | 'UNKNOWN'
    """
    html = (await page.content()).lower()
    # PG iframe 탐지
    for fr in page.frames:
        with contextlib.suppress(Exception):
            src = (await fr.evaluate("() => window.location.href")).lower()
            if any(k in src for k in ("inicis","kcp","toss","kakaopay","lguplus","lgup","naverpay","npay")):
                return "PG"

    # 결제/체크아웃 UI 신호
    has_pay_btn = await page.locator("button:has-text('결제'), button:has-text('결제하기')").count() > 0
    has_checkout_words = any(k in html for k in ("결제수단","주문자","예매자","약관","전체동의","최종 결제"))
    if has_pay_btn or has_checkout_words:
        return "CHECKOUT"

    # 자유석 수량
    if await page.locator("input[name*=qty], input[name='totalCnt'], input[name='orderCnt'], select[name*='cnt'], select[name*='qty']").count() > 0:
        if any(k in html for k in ("수량","매수","인원","총 매수")):
            return "NRS_QTY"

    # 좌석선택
    if any(k in html for k in ("좌석", "seat", "block", "구역")) and \
       (await page.locator("#seatMap, canvas.seat, .seat-map, #seatLayer").count() > 0 or
        await page.locator("button:has-text('좌석선택')").count() > 0):
        return "SEAT"

    return "UNKNOWN"

# ===== UI 액션 =====
async def nrs_to_checkout(page: Page, show: Show) -> bool:
    """
    자유석: 수량 1 → 다음/구매/예매 버튼 눌러 CHECKOUT/PG 단계까지 이동
    """
    try:
        await page.goto(f"{ONESTOP_SITE}/ko/onestop/rs?prodSeq={show.prodSeq}&sdSeq={show.sdSeq}",
                        wait_until="domcontentloaded", timeout=STEP_TIMEOUT_MS)
        # 수량 1
        for sel in ["input[name*=qty]", "input[name='totalCnt']", "input[name='orderCnt']",
                    "select[name*='cnt']", "select[name*='qty']"]:
            loc = page.locator(sel).first
            if await loc.count():
                try:
                    tag = await loc.evaluate("(e)=>e.tagName.toLowerCase()")
                    if tag == "select":
                        await loc.select_option(index=1)  # 보통 1매가 index 1
                    else:
                        await loc.fill("1")
                    break
                except Exception:
                    pass

        # 다음/구매/예매 버튼
        for sel in ["button:has-text('다음')","button:has-text('구매')","button:has-text('예매')",
                    "a:has-text('다음')","a:has-text('구매')","a:has-text('예매')"]:
            loc = page.locator(sel).first
            if await loc.count():
                with contextlib.suppress(Exception):
                    await loc.click()
                    break

        # 단계 대기
        for _ in range(12):
            st = await detect_stage(page)
            if st in ("CHECKOUT","PG"):
                return True
            await asyncio.sleep(1.0)
        return False
    except Exception:
        return False

async def seat_to_checkout_try(page: Page, show: Show) -> bool:
    """
    지정석: 좌석화면 진입 후 '다음/결제' 등 누르며 CHECKOUT/PG까지 시도
    """
    try:
        await page.goto(f"{ONESTOP_SITE}/ko/onestop/rs/seat?prodSeq={show.prodSeq}&sdSeq={show.sdSeq}",
                        wait_until="domcontentloaded", timeout=STEP_TIMEOUT_MS)

        # 좌석 자동선택은 사이트별로 달라서 여기선 스킵(신호만 보고 다음/결제 시도)
        for sel in ["button:has-text('다음')","button:has-text('결제')","a:has-text('다음')","a:has-text('결제')"]:
            loc = page.locator(sel).first
            if await loc.count():
                with contextlib.suppress(Exception):
                    await loc.click()
                    break

        for _ in range(12):
            st = await detect_stage(page)
            if st in ("CHECKOUT","PG"):
                return True
            await asyncio.sleep(1.0)
        return False
    except Exception:
        return False

# ===== 러너 =====
class Runner:
    def __init__(self, ctx: BrowserContext, page: Page):
        self.ctx = ctx
        self.page = page  # 로그인만 이 페이지로
        self.sem = asyncio.Semaphore(CONCURRENCY)
        self.results = []
        self.seed_ss_biff: Dict[str,str] = {}
        self.seed_ss_onestop: Dict[str,str] = {}

    async def process_sd(self, sdCode: str):
        async with self.sem:
            local_page = await self.ctx.new_page()   # ★ 건별 전용 페이지
            # ★ 새 탭에 포털 세션부터 심기 (origin: biff)
            await prime_session_storage(local_page, PORTAL_SITE, self.seed_ss_biff)

            # ★ onestop 세션도 있으면 미리 심어 (첫 건 이후 캐시됨)
            if self.seed_ss_onestop:
                await prime_session_storage(local_page, ONESTOP_SITE, self.seed_ss_onestop)
            title = "(제목미상)"
            try:
                show = await filmapi_get_show(self.ctx, sdCode)
                title = (show.title or "(제목미상)").strip()
                log_info(f"🎬 [{sdCode}] {show.title}")
                # ★ SSO 브릿지(직렬화) — onestop 세션/쿠키/CSRF 경로 활성화
                async with SSO_LOCK:
                    local_page = await bridge_sso(local_page, show)   # ★★★ 새 탭 받을 수 있음
                    setattr(local_page, "_sso_ready", True)           # 이 탭에서만 onestop/API 허용
                    if not self.seed_ss_onestop:
                        with contextlib.suppress(Exception):
                            self.seed_ss_onestop = await dump_session_storage(local_page)

                    # onestop 세션을 시드로 캐시 (다음 탭부터는 선주입)
                    if not self.seed_ss_onestop:
                        with contextlib.suppress(Exception):
                            self.seed_ss_onestop = await dump_session_storage(local_page)
                # RS/SEAT 워밍 (전용 페이지에서 수행)
                tok = await warmup_rs(local_page, show)
                tok = await warmup_seat(local_page, show, tok)

                # CSRF 확보 (전용 페이지의 쿠키/리퍼러 컨텍스트 사용)
                if not tok.rs_csrf or not tok.seat_csrf:
                    t = await ensure_csrf_token(local_page, show)
                    log_info(f"[{show.sdCode}] rs_csrf len={len(t) if t else 0}")
                    if not tok.rs_csrf:   tok.rs_csrf = t
                    if not tok.seat_csrf: tok.seat_csrf = t
                if not tok.rs_csrf:
                    raise RuntimeError("missing CSRF after warmup: rs_csrf")

                # 프라이밍 & 좌석 통계는 request API로
                await prime_rs(self.ctx, show, tok)
                plan, total, remain, bycat = await seat_stats(self.ctx, show, tok)
                log_info(f"ℹ️  [{sdCode}] {show.title} | {show.venue} {show.hall} | {show.perfDate} | plan={plan} | 총={total} 잔여={remain}")

                # 단계 이동은 전용 페이지로 진행
                ok = False
                if plan == "NRS" and remain > 0:
                    ok = await nrs_to_checkout(local_page, show)
                else:
                    ok = await seat_to_checkout_try(local_page, show)

                if ok:
                    st = await detect_stage(local_page)
                    self.results.append((sdCode, title, f"(stage={st}) {local_page.url}"))
                else:
                    raise RuntimeError("결제단계 진입 실패(좌석/버튼 불가)")
            except Exception as e:
                log_err(f"❌ [{sdCode}] 에러: {e}")
                self.results.append((sdCode, title, f"(예외: {e})"))
            finally:
                with contextlib.suppress(Exception):
                    await local_page.close()

    async def run(self, sdCodes: List[str]):
        # 공용 self.page는 '로그인' 전용으로만 사용
        await wait_for_login(self.ctx, self.page)
        # 로그인/SSO 기반 "시드 탭"의 sessionStorage 캐시
        self.seed_ss_biff = await dump_session_storage(self.page)   # 포털(biff)용
        self.seed_ss_onestop = {}  # onestop은 첫 브릿지 이후 캐시
        tasks = [asyncio.create_task(self.process_sd(sd)) for sd in sdCodes]
        await asyncio.gather(*tasks)


# ===== 메인 =====
async def main():
    if len(sys.argv) < 2:
        print("사용법: python bf.py <sdCode1> <sdCode2> ...")
        return
    sdCodes = [s.strip() for s in sys.argv[1:] if s.strip()]

    log_info(f"DEBUG Using proactor: IocpProactor")

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"]
            )
        except Exception:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                args=["--disable-blink-features=AutomationControlled"]
            )
        try:
            ctx = await browser.new_context(
                locale="ko-KR",
                timezone_id="Asia/Seoul",
                extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"},
                record_har_path=str(HAR_PATH),
                record_har_omit_content=False,
            )
        except TypeError:
            ctx = await browser.new_context(
                locale="ko-KR",
                timezone_id="Asia/Seoul",
                extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"},
            )

        with contextlib.suppress(Exception):
            await ctx.tracing.start(screenshots=True, snapshots=True, sources=True)

        page = await ctx.new_page()
        with contextlib.suppress(Exception):
            await page.bring_to_front()

        runner = Runner(ctx, page)
        await runner.run(sdCodes)

        print("\n" + "─"*72)
        print("📊 결과 요약")
        for sd, title, s in runner.results:
            if s.startswith("(stage="):
                print(f"  ✅ [{sd}] {title or '(제목미상)'}  →  {s}")
            else:
                print(f"  ❌ [{sd}] {title or '(제목미상)'}  {s}")
        print("─"*72)

        with contextlib.suppress(Exception):
            await ctx.tracing.stop(path=str(TRACE_PATH))
        await ctx.close()
        await browser.close()

        log_info("\n── 아티팩트 저장 위치 ───────────────────────")
        log_info(f"  • 로그: {LOG_PATH}")
        log_info(f"  • 이벤트(JSONL): {JSONL_PATH}")
        log_info(f"  • HAR: {HAR_PATH}")
        log_info(f"  • Trace: {TRACE_PATH}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass

# 최신 네이버 블로그 URL 수집 + 본문 추출
# 결과를 (posts, failures) 튜플로 반환 → 호출자(DAG)가 Kafka로 publish.
#
# [성능 개선 요약] 기존 30분+ → 목표 3~5분
#
# 핵심 변경 3가지:
#   1. Playwright 제거 → requests 전용
#      - m.blog.naver.com 은 서버 사이드 렌더링(SSR) 페이지.
#        첫 번째 HTTP 응답 HTML에 본문이 이미 포함되어 있어 JS 실행 불필요.
#      - Chromium 기동(~1s) + networkidle 대기(~3s) = 건당 4s 절감.
#
#   2. ThreadPoolExecutor 병렬 fetch
#      - HTTP GET은 I/O bound: 네트워크 대기 중 GIL이 해제되므로
#        멀티스레딩으로 CPU 추가 없이 병렬 처리 가능.
#      - asyncio + aiohttp 대신 ThreadPoolExecutor를 선택한 이유:
#        requests 라이브러리가 동기 API → 전체 재작성 없이 바로 적용 가능.
#      - 50 workers 기준 기대 성능:
#          순차 1000개 × 평균 1s = 1000s (16분)
#          병렬 1000개 ÷ 50 workers × 1s ≈ 20s  (50배 단축)
#
#   3. URL 정규화에서 HTTP fallback 제거
#      - 기존: regex 미매칭 URL에 대해 HTTP 요청으로 리다이렉트 추적
#        → 발견 단계 URL 수 × 추가 요청 = 수천 건의 불필요한 HTTP 호출
#      - 변경: 순수 문자열 처리만 사용, 미매칭은 원본 반환

import os
import re
import time
import html
import argparse
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlunparse, quote

from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

KST = timezone(timedelta(hours=9))

load_dotenv()

# 모바일 UA → m.blog.naver.com SSR 페이지 반환 유도
UA = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0 Mobile Safari/537.36"
)

# 5초 이내 응답 없으면 즉시 포기 — 느린 호스트/DNS 실패로 인한 대기 차단 방지
FETCH_TIMEOUT = 5

# I/O bound(HTTP GET): 네트워크 대기 중 GIL 해제 → 고수치 worker 사용 가능
# 50 workers × 5s timeout = 최악 250s, 평균 1s 응답 기준 1000 URL ≈ 20s
MAX_WORKERS = 50

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 1) Token sampling pool
# ──────────────────────────────────────────────
def build_token_pool(seed: int = 42) -> List[str]:
    random.seed(seed)

    hangul_single = list("가나다라마바사아자차카타파하") + list("이그저너내우리더또")
    particles = ["은", "는", "이", "가", "을", "를", "에", "에서", "로", "으로",
                 "와", "과", "도", "만", "의", "한", "하다", "했다", "합니다"]
    numbers = [str(i) for i in range(1, 32)] + ["2026", "2025", "02", "03", "10", "100"]
    common_words = [
        "오늘", "어제", "내일", "주말", "아침", "점심", "저녁",
        "일상", "기록", "후기", "리뷰", "추천", "정리", "공유",
        "사진", "맛집", "여행", "카페", "운동", "공부", "책", "영화",
    ]
    english_single = list("aeiou") + ["t", "n", "s", "r", "l"]

    pool = list(set(hangul_single + particles + numbers + common_words + english_single))
    pool = [p.strip() for p in pool if p.strip()]
    random.shuffle(pool)
    return pool


# ──────────────────────────────────────────────
# 2) Naver Search API call
# ──────────────────────────────────────────────
def naver_blog_search(
    query: str, display: int, start: int, client_id: str, client_secret: str
) -> Dict:
    url = "https://openapi.naver.com/v1/search/blog.json"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
        "User-Agent": UA,
    }
    params = {"query": query, "display": display, "start": start, "sort": "date"}
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


# ──────────────────────────────────────────────
# 3) URL normalization helpers
# ──────────────────────────────────────────────
def normalize_url_light(url: str) -> str:
    url = url.strip()
    try:
        u = urlparse(url)
        q = parse_qs(u.query)
        keep = {}
        for k in ["blogId", "logNo", "Redirect", "proxyReferer"]:
            if k in q and q[k]:
                keep[k] = q[k][0]
        query = "&".join([f"{k}={quote(v)}" for k, v in keep.items()])
        u2 = u._replace(query=query, fragment="")
        return urlunparse(u2)
    except Exception:
        return url


def to_mobile_naver_blog_url(url: str) -> str:
    """
    네이버 블로그 URL을 m.blog.naver.com 형식으로 변환.

    순수 문자열/regex 처리만 사용 — HTTP 요청 없음.
    (기존 HTTP fallback 제거: 발견 단계에서 URL마다 추가 요청이 발생하던 문제 해결)
    """
    url = normalize_url_light(url)

    if "m.blog.naver.com" in url:
        return url

    # 쿼리스트링 패턴: ?blogId=xxx&logNo=yyy
    m = re.search(r"blogId=([^&]+).*logNo=(\d+)", url)
    if m:
        return f"https://m.blog.naver.com/{m.group(1)}/{m.group(2)}"

    # 경로 패턴: blog.naver.com/blogId/logNo
    m = re.search(r"blog\.naver\.com/([^/?]+)/(\d+)", url)
    if m:
        return f"https://m.blog.naver.com/{m.group(1)}/{m.group(2)}"

    # 매칭 실패 시 원본 반환 (HTTP fallback 없음)
    return url


# ──────────────────────────────────────────────
# 4) Data model
# ──────────────────────────────────────────────
@dataclass
class DiscoveredItem:
    url: str
    postdate: Optional[str] = None
    title: Optional[str] = None
    bloggername: Optional[str] = None
    bloggerlink: Optional[str] = None
    token: Optional[str] = None
    discovered_at: str = ""


# ──────────────────────────────────────────────
# 5) HTML parsing
# ──────────────────────────────────────────────
def parse_mobile_post_html(html_text: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html_text, "lxml")

    title = None
    for sel in ["meta[property='og:title']", "div.se-title-text span", "h3.tit_h3", "title"]:
        el = soup.select_one(sel)
        if not el:
            continue
        title = el.get("content") if el.name == "meta" else el.get_text(" ", strip=True)
        if title:
            break

    content_el = None
    for sel in ["div.se-main-container", "div#viewTypeSelector", "div#postViewArea", "div.post_ct"]:
        content_el = soup.select_one(sel)
        if content_el:
            break

    content_text = None
    if content_el:
        for tag in content_el(["script", "style", "noscript"]):
            tag.decompose()
        content_text = content_el.get_text("\n", strip=True)

    return {
        "title": html.unescape(title) if title else None,
        "content_text": content_text,
    }


# ──────────────────────────────────────────────
# 6) 단위 fetch 함수
# ──────────────────────────────────────────────
def fetch_post(url: str) -> Dict:
    """
    단일 블로그 포스트 본문 수집 (requests 전용).

    - timeout=5s: ERR_NAME_NOT_RESOLVED·느린 호스트 즉시 포기
    - 최대 1회 재시도: 일시적 연결 오류 대응
    - Playwright 없이 동작하는 이유:
        m.blog.naver.com은 SSR 페이지 — 첫 HTML 응답에 본문 포함.
        JS 실행·브라우저 렌더링 불필요.
    """
    headers = {
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    }

    last_exc: Optional[Exception] = None
    for attempt in range(2):  # 첫 시도 + 1회 재시도
        try:
            r = requests.get(url, headers=headers, timeout=FETCH_TIMEOUT, allow_redirects=True)
            r.raise_for_status()
            parsed = parse_mobile_post_html(r.text)
            return {"final_url": r.url, **parsed}
        except Exception as e:
            last_exc = e
            if attempt == 0:
                time.sleep(0.3)  # 재시도 전 짧은 대기

    raise last_exc  # type: ignore[misc]


# ──────────────────────────────────────────────
# 7) 병렬 fetch 함수
# ──────────────────────────────────────────────
def parallel_fetch(
    items: List[DiscoveredItem],
    max_workers: int = MAX_WORKERS,
) -> Tuple[List[dict], List[dict]]:
    """
    ThreadPoolExecutor를 사용한 병렬 본문 수집.

    성능 근거:
      - I/O bound 작업(HTTP GET)은 GIL이 네트워크 대기 중 해제됨
        → 멀티스레딩으로 실질적인 병렬 처리 가능
      - as_completed() 사용: 응답이 빠른 URL부터 처리 (느린 URL에 묶이지 않음)
      - 실패 URL은 즉시 failures에 기록하고 다음 URL로 진행
    """
    posts: List[dict] = []
    failures: List[dict] = []
    total = len(items)
    workers = min(max_workers, total)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_item = {executor.submit(fetch_post, it.url): it for it in items}

        done = 0
        for future in as_completed(future_to_item):
            it = future_to_item[future]
            done += 1

            try:
                fetched = future.result()
                posts.append({
                    "schema_version": "1.0",
                    "url": it.url,
                    "title": fetched.get("title") or it.title,
                    "content_text": fetched.get("content_text"),
                    "postdate": _fmt_postdate(it.postdate),
                    "discovered_at": it.discovered_at,
                })
            except Exception as e:
                logger.warning("[fetch] 실패 url=%s error=%s", it.url, e)
                failures.append({"url": it.url, "error": str(e)})

            # 10% 단위 진행 로그
            interval = max(1, total // 10)
            if done % interval == 0 or done == total:
                logger.info(
                    "[fetch] 진행 %d/%d | 성공=%d 실패=%d",
                    done, total, len(posts), len(failures),
                )

    return posts, failures


def _fmt_postdate(postdate: Optional[str]) -> Optional[str]:
    """YYYYMMDD → YYYY-MM-DD 변환. 이미 변환됐거나 None이면 그대로 반환."""
    if postdate and re.match(r"^\d{8}$", postdate):
        return f"{postdate[:4]}-{postdate[4:6]}-{postdate[6:]}"
    return postdate


# ──────────────────────────────────────────────
# 8) 메인 파이프라인: URL 발견 + 병렬 본문 추출
# ──────────────────────────────────────────────
def discover_latest(
    client_id: str,
    client_secret: str,
    target_unique: int = 1000,
    display: int = 100,
    starts: List[int] = None,
    tokens_per_run: int = 80,
    seed: int = 42,
    sleep_s: float = 0.1,
    max_fetch: int = 1000,
    fetch_sleep_s: float = 0.0,  # deprecated: 병렬 처리로 대체. 하위 호환을 위해 유지.
) -> Tuple[List[dict], List[dict]]:
    """
    네이버 블로그 최신 포스트 수집 + 본문 추출.

    완전 무상태(stateless) 함수 — 파일 시스템·DB 접근 없음.
    중복 제거는 Kafka key(URL) 기반으로 다운스트림에서 처리한다.

    Returns:
        (posts, failures)
          posts:    본문 추출 성공 포스트 dict 리스트
          failures: 본문 추출 실패 항목 dict 리스트
    """
    if starts is None:
        starts = [1, 101, 201]

    token_pool = build_token_pool(seed=seed)
    random.shuffle(token_pool)
    tokens = token_pool[:min(tokens_per_run, len(token_pool))]

    uniq: Dict[str, DiscoveredItem] = {}
    discovered_at = datetime.now(KST).isoformat()

    # ── URL 발견 단계 ─────────────────────────────
    api_calls = 0
    for token in tokens:
        for st in starts:
            if len(uniq) >= target_unique:
                break
            api_calls += 1
            try:
                data = naver_blog_search(
                    token, display=display, start=st,
                    client_id=client_id, client_secret=client_secret,
                )
            except Exception as e:
                logger.warning("[discover] API 호출 실패 token=%s start=%d error=%s", token, st, e)
                continue

            for it in data.get("items", []):
                link = it.get("link")
                if not link or "blog.naver.com" not in link:
                    continue

                norm = to_mobile_naver_blog_url(link)
                if norm in uniq:
                    continue

                title = it.get("title") or None
                if title:
                    title = re.sub(r"<[^>]+>", "", title)
                    title = html.unescape(title)

                uniq[norm] = DiscoveredItem(
                    url=norm,
                    postdate=it.get("postdate") or None,
                    title=title,
                    bloggername=it.get("bloggername") or None,
                    bloggerlink=it.get("bloggerlink") or None,
                    token=token,
                    discovered_at=discovered_at,
                )

                if len(uniq) >= target_unique:
                    break

            time.sleep(sleep_s)  # Naver API 레이트 리밋 방지

        if len(uniq) >= target_unique:
            break

    logger.info("[discover] API calls=%d, unique_urls=%d", api_calls, len(uniq))

    items = list(uniq.values())
    if not items:
        logger.info("[discover] 신규 URL이 없음. 빈 결과 반환.")
        return [], []

    # ── 병렬 본문 추출 단계 ───────────────────────
    fetch_items = items[:max_fetch]
    logger.info(
        "[discover] 병렬 fetch 시작: %d URLs, workers=%d, timeout=%ds",
        len(fetch_items), MAX_WORKERS, FETCH_TIMEOUT,
    )

    t0 = time.monotonic()
    posts, failures = parallel_fetch(fetch_items, max_workers=MAX_WORKERS)
    elapsed = time.monotonic() - t0

    rate = len(fetch_items) / elapsed if elapsed > 0 else 0
    logger.info(
        "[discover] 완료: posts=%d failures=%d elapsed=%.1fs (%.1f URL/s)",
        len(posts), len(failures), elapsed, rate,
    )

    return posts, failures


# ──────────────────────────────────────────────
# 9) Standalone 실행 (테스트용)
# ──────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--target",    type=int,   default=1000)
    ap.add_argument("--display",   type=int,   default=100)
    ap.add_argument("--starts",    type=str,   default="1,101,201")
    ap.add_argument("--tokens",    type=int,   default=80)
    ap.add_argument("--seed",      type=int,   default=42)
    ap.add_argument("--sleep",     type=float, default=0.1)
    ap.add_argument("--max-fetch", type=int,   default=1000)
    ap.add_argument("--workers",   type=int,   default=MAX_WORKERS)
    args = ap.parse_args()

    client_id     = os.getenv("NAVER_CLIENT_ID", "").strip()
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise SystemExit("환경변수 NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 를 설정해 주세요.")

    starts = [int(x.strip()) for x in args.starts.split(",") if x.strip()]

    posts, failures = discover_latest(
        client_id=client_id,
        client_secret=client_secret,
        target_unique=args.target,
        display=min(args.display, 100),
        starts=starts,
        tokens_per_run=args.tokens,
        seed=args.seed,
        sleep_s=args.sleep,
        max_fetch=args.max_fetch,
    )

    logger.info("결과: posts=%d failures=%d", len(posts), len(failures))


if __name__ == "__main__":
    main()

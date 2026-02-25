# 최신 네이버 블로그 URL 수집 + 본문 추출
# 결과를 (posts, failures) 튜플로 반환 → 호출자(DAG)가 Kafka로 publish.

import os
import re
import time
import html
import argparse
import random
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlunparse, quote

from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

KST = timezone(timedelta(hours=9))

load_dotenv()

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"

logger = logging.getLogger(__name__)


# -----------------------------
# 1) Token sampling pool
# -----------------------------
def build_token_pool(seed: int = 42) -> List[str]:
    random.seed(seed)

    hangul_single = list("가나다라마바사아자차카타파하") + list("이그저너내우리더또")
    particles = ["은","는","이","가","을","를","에","에서","로","으로","와","과","도","만","의","한","하다","했다","합니다"]
    numbers = [str(i) for i in range(1, 32)] + ["2026", "2025", "02", "03", "10", "100"]
    common_words = [
        "오늘","어제","내일","주말","아침","점심","저녁",
        "일상","기록","후기","리뷰","추천","정리","공유",
        "사진","맛집","여행","카페","운동","공부","책","영화"
    ]
    english_single = list("aeiou") + ["t","n","s","r","l"]

    pool = list(set(hangul_single + particles + numbers + common_words + english_single))
    pool = [p.strip() for p in pool if p.strip()]
    random.shuffle(pool)
    return pool


# -----------------------------
# 2) Naver Search API call
# -----------------------------
def naver_blog_search(query: str, display: int, start: int, client_id: str, client_secret: str) -> Dict:
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


# -----------------------------
# 3) URL normalization helpers
# -----------------------------
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
    url = normalize_url_light(url)

    if "m.blog.naver.com" in url:
        return url

    m = re.search(r"blogId=([^&]+).*logNo=(\d+)", url)
    if m:
        blog_id, log_no = m.group(1), m.group(2)
        return f"https://m.blog.naver.com/{blog_id}/{log_no}"

    m = re.search(r"https?://blog\.naver\.com/([^/]+)/(\d+)", url)
    if m:
        blog_id, log_no = m.group(1), m.group(2)
        return f"https://m.blog.naver.com/{blog_id}/{log_no}"

    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=10, allow_redirects=True)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        iframe = soup.select_one("iframe#mainFrame")
        if iframe and iframe.get("src"):
            src = iframe["src"]
            m2 = re.search(r"blogId=([^&]+).*logNo=(\d+)", src)
            if m2:
                blog_id, log_no = m2.group(1), m2.group(2)
                return f"https://m.blog.naver.com/{blog_id}/{log_no}"
    except Exception:
        pass

    return url


# -----------------------------
# 4) Data models
# -----------------------------
@dataclass
class DiscoveredItem:
    url: str
    postdate: Optional[str] = None
    title: Optional[str] = None
    bloggername: Optional[str] = None
    bloggerlink: Optional[str] = None
    token: Optional[str] = None
    discovered_at: str = ""


# -----------------------------
# 5) Content extraction
# -----------------------------
def parse_mobile_post_html(html_text: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html_text, "lxml")

    title = None
    for sel in ["meta[property='og:title']", "div.se-title-text span", "h3.tit_h3", "title"]:
        el = soup.select_one(sel)
        if not el:
            continue
        if el.name == "meta":
            title = el.get("content")
        else:
            title = el.get_text(" ", strip=True)
        if title:
            break

    content_el = None
    for sel in ["div.se-main-container", "div#viewTypeSelector", "div#postViewArea", "div.post_ct"]:
        content_el = soup.select_one(sel)
        if content_el:
            break

    content_text = None
    if content_el:
        for t in content_el(["script", "style", "noscript"]):
            t.decompose()
        content_text = content_el.get_text("\n", strip=True)

    return {"title": html.unescape(title) if title else None, "content_text": content_text}


def fetch_by_requests(url: str) -> Dict:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=20, allow_redirects=True)
    r.raise_for_status()
    parsed = parse_mobile_post_html(r.text)
    return {"method": "requests", "final_url": r.url, **parsed}


def fetch_by_playwright(url: str) -> Dict:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=UA)
        page.goto(url, wait_until="networkidle", timeout=30000)
        html_text = page.content()
        browser.close()
    parsed = parse_mobile_post_html(html_text)
    return {"method": "playwright", "final_url": url, **parsed}


def fetch_post_content(url: str) -> Dict:
    target = to_mobile_naver_blog_url(url)
    try:
        res = fetch_by_requests(target)
        if not res.get("content_text"):
            raise RuntimeError("empty content_text")
        return {"url": target, **res}
    except Exception as e:
        res = fetch_by_playwright(target)
        return {"url": target, **res, "fallback_reason": str(e)}


# ──────────────────────────────────────────────
# 7) 메인 파이프라인: 발견 + 본문 추출
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
    max_fetch: int = 200,
    fetch_sleep_s: float = 0.2,
) -> Tuple[List[dict], List[dict]]:
    """
    네이버 블로그 최신 포스트를 수집하고 본문을 추출하여 반환.

    완전 무상태(stateless) 함수 — 파일 시스템·DB 접근 없음.
    중복 제거는 Kafka key(URL) 기반으로 다운스트림에서 처리한다.

    Returns:
        (posts, failures)
          posts:    본문 추출 성공한 포스트 dict 리스트
          failures: 본문 추출 실패한 항목 dict 리스트
    """
    if starts is None:
        starts = [1, 101, 201]

    token_pool = build_token_pool(seed=seed)
    random.shuffle(token_pool)
    tokens = token_pool[:min(tokens_per_run, len(token_pool))]

    uniq: Dict[str, DiscoveredItem] = {}
    discovered_at = datetime.now(KST).isoformat()

    # ── URL 발견 ─────────────────────────────────
    api_calls = 0
    for token in tokens:
        for st in starts:
            if len(uniq) >= target_unique:
                break
            api_calls += 1
            data = naver_blog_search(
                token, display=display, start=st,
                client_id=client_id, client_secret=client_secret,
            )

            for it in data.get("items", []):
                link = it.get("link")
                if not link:
                    continue
                if "blog.naver.com" not in link and "m.blog.naver.com" not in link:
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

            time.sleep(sleep_s)

        if len(uniq) >= target_unique:
            break

    logger.info("[discover] API calls=%d, unique_urls=%d", api_calls, len(uniq))

    items = list(uniq.values())

    if not items:
        logger.info("[discover] 신규 URL이 없음. 빈 결과 반환.")
        return [], []

    # ── 본문 추출 ─────────────────────────────────
    posts: List[dict] = []
    failures: List[dict] = []

    for idx, it in enumerate(items[:max_fetch], start=1):
        try:
            fetched = fetch_post_content(it.url)
            post_record = {
                "url":             it.url,
                "postdate":        it.postdate,
                "discovered_at":   it.discovered_at,
                "method":          fetched.get("method"),
                "final_url":       fetched.get("final_url"),
                "title":           fetched.get("title") or it.title,
                "content_text":    fetched.get("content_text"),
                "fallback_reason": fetched.get("fallback_reason"),
            }
            posts.append(post_record)
        except Exception as e:
            logger.warning("[fetch] 실패 url=%s error=%s", it.url, e)
            failures.append({"url": it.url, "error": str(e)})

        time.sleep(fetch_sleep_s)

    logger.info(
        "[discover] 완료: posts=%d failures=%d (total=%d)",
        len(posts), len(failures), len(items),
    )

    return posts, failures


# ──────────────────────────────────────────────
# 8) Standalone 실행 (테스트용)
# ──────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    ap = argparse.ArgumentParser()
    ap.add_argument("--target",         type=int,   default=1000)
    ap.add_argument("--display",        type=int,   default=100)
    ap.add_argument("--starts",         type=str,   default="1,101,201")
    ap.add_argument("--tokens",         type=int,   default=80)
    ap.add_argument("--seed",           type=int,   default=42)
    ap.add_argument("--sleep",       type=float, default=0.1)
    ap.add_argument("--max-fetch",   type=int,   default=200)
    ap.add_argument("--fetch-sleep", type=float, default=0.2)
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
        fetch_sleep_s=args.fetch_sleep,
    )

    logger.info("결과: posts=%d failures=%d", len(posts), len(failures))


if __name__ == "__main__":
    main()

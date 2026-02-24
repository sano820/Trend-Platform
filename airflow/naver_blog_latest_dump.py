# 1000개 URL 수집 + (옵션) 본문 저장
# 기본 동작: 1000개 목표로 URL/메타 CSV/JSON 저장
# 옵션: --fetch-content 켜면, 본문도 JSON 파일로 저장(시간 오래 걸릴 수 있음)
# → 네가 말한 조건 4를 위해, 본문 저장이 벅차면 메타만 저장해도 결과가 남게 설계

import os
import re
import csv
import json
import time
import html
import argparse
import hashlib
import random
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlunparse, quote

from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup

# timezone constants
KST = timezone(timedelta(hours=9))

load_dotenv()

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"

# -----------------------------
# 0) 수집한 파일 저장 위치
# -----------------------------
AIRFLOW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # .../airflow
PROJECT_ROOT = os.path.abspath(os.path.join(AIRFLOW_DIR, ".."))              # .../Trend-Platform
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# -----------------------------
# 1) Token sampling pool
# -----------------------------
def build_token_pool(seed: int = 42) -> List[str]:
    random.seed(seed)

    # 흔한 한글/조사/숫자/일상단어/영문 일부 섞기
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
    """
    Naver Blog Search API
    - query: required   -> 검색창에 입력하는 단어
    - display: max 100  -> 한 번 API 호출에서 몇개 결과 받을지(최대 100, 기본 10)
    - start: max 1000   -> 한 검색어에 대해 최대 1000번째 결과까지 접근 가능
    - sort: date        -> 최신 순 정렬
    """
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
    """프래그먼트 제거 + 일부 추적성 파라미터 제거(가능한 범위)"""
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
    가능한 한 https://m.blog.naver.com/{blogId}/{logNo} 형태로 정규화.
    실패하면 원본(정규화된) 반환.
    """
    url = normalize_url_light(url)

    if "m.blog.naver.com" in url:
        return url

    # PostView: ...PostView.naver?blogId=...&logNo=...
    m = re.search(r"blogId=([^&]+).*logNo=(\d+)", url)
    if m:
        blog_id, log_no = m.group(1), m.group(2)
        return f"https://m.blog.naver.com/{blog_id}/{log_no}"

    # PC direct: https://blog.naver.com/{blogId}/{logNo}
    m = re.search(r"https?://blog\.naver\.com/([^/]+)/(\d+)", url)
    if m:
        blog_id, log_no = m.group(1), m.group(2)
        return f"https://m.blog.naver.com/{blog_id}/{log_no}"

    # iframe mainFrame 케이스
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
    postdate: Optional[str] = None  # YYYYMMDD (API field)
    title: Optional[str] = None
    bloggername: Optional[str] = None
    bloggerlink: Optional[str] = None
    token: Optional[str] = None
    discovered_at: str = ""         # ISO time


# -----------------------------
# 5) Discovery: get up to N unique URLs
# -----------------------------
def discover_latest(
    client_id: str,
    client_secret: str,
    target_unique: int = 1000,
    display: int = 100,
    starts: List[int] = [1, 101, 201],  # 얕게 파되 토큰 다양성으로 커버
    tokens_per_run: int = 80,
    seed: int = 42,
    sleep_s: float = 0.1,
) -> List[DiscoveredItem]:
    """
    토큰 샘플링으로 최신글처럼 URL을 넓게 수집.
    - 중복 제거는 (모바일 정규화 URL) 기준으로 수행.
    """
    token_pool = build_token_pool(seed=seed)
    random.shuffle(token_pool)
    tokens = token_pool[:min(tokens_per_run, len(token_pool))]

    uniq: Dict[str, DiscoveredItem] = {}
    discovered_at = datetime.now(KST).isoformat()   

    api_calls = 0
    for token in tokens:
        for st in starts:
            if len(uniq) >= target_unique:
                break
            api_calls += 1
            data = naver_blog_search(token, display=display, start=st, client_id=client_id, client_secret=client_secret)

            for it in data.get("items", []):
                link = it.get("link")
                if not link:
                    continue

                # 네이버 블로그 링크만 우선 수집 (원하면 제거 가능)
                if "blog.naver.com" not in link and "m.blog.naver.com" not in link:
                    continue

                norm = to_mobile_naver_blog_url(link)

                if norm in uniq:
                    continue

                title = it.get("title") or None
                if title:
                    # <b> 태그 제거(문서에 title/description에서 <b>로 감싼다고 되어 있음)
                    title = re.sub(r"<[^>]+>", "", title)
                    title = html.unescape(title)

                item = DiscoveredItem(
                    url=norm,
                    postdate=it.get("postdate") or None,
                    title=title,
                    bloggername=it.get("bloggername") or None,
                    bloggerlink=it.get("bloggerlink") or None,
                    token=token,
                    discovered_at=discovered_at,
                )
                uniq[norm] = item

                if len(uniq) >= target_unique:
                    break

            time.sleep(sleep_s)

        if len(uniq) >= target_unique:
            break

    print(f"[discover] API calls={api_calls}, unique_urls={len(uniq)}")
    return list(uniq.values())


# -----------------------------
# 6) Content extraction: scraping -> rendering fallback
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

# -----------------------------
# 7-1) SQLite 유틸 함수 추가
# -----------------------------
def _seen_init(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS seen_urls (
        url TEXT PRIMARY KEY,
        first_seen_at TEXT NOT NULL
    )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_seen_first_seen ON seen_urls(first_seen_at)")
    conn.commit()

def _seen_prune(conn: sqlite3.Connection, ttl_hours: int, now_iso: str):
    # now_iso는 ISO 문자열(예: 2026-02-24T12:00:00+09:00)
    # SQLite에서는 문자열 비교가 안전하려면 동일 포맷을 써야 해서 ISO를 그대로 저장/비교
    # prune 기준 시각 계산은 파이썬에서.
    from datetime import datetime, timedelta
    now_dt = datetime.fromisoformat(now_iso)
    cutoff = (now_dt - timedelta(hours=ttl_hours)).isoformat()
    conn.execute("DELETE FROM seen_urls WHERE first_seen_at < ?", (cutoff,))
    conn.commit()

def _seen_filter_new(urls: List[str], seen_db_path: str, ttl_hours: int, now_iso: str) -> Tuple[List[str], int]:
    """
    urls 중에서 TTL 기간 내 이미 본 url은 제외하고 신규만 반환.
    반환: (new_urls, skipped_count)
    """
    if not seen_db_path:
        return urls, 0

    os.makedirs(os.path.dirname(seen_db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(seen_db_path, timeout=30)
    try:
        _seen_init(conn)
        _seen_prune(conn, ttl_hours=ttl_hours, now_iso=now_iso)

        # 존재 여부 체크
        new_urls = []
        skipped = 0
        for u in urls:
            row = conn.execute("SELECT 1 FROM seen_urls WHERE url = ? LIMIT 1", (u,)).fetchone()
            if row:
                skipped += 1
            else:
                new_urls.append(u)
        return new_urls, skipped
    finally:
        conn.close()

def _seen_mark(urls: List[str], seen_db_path: str, now_iso: str) -> None:
    if not seen_db_path or not urls:
        return
    conn = sqlite3.connect(seen_db_path, timeout=30)
    try:
        _seen_init(conn)
        # 신규만 insert (PRIMARY KEY 충돌 시 무시)
        conn.executemany(
            "INSERT OR IGNORE INTO seen_urls(url, first_seen_at) VALUES(?, ?)",
            [(u, now_iso) for u in urls],
        )
        conn.commit()
    finally:
        conn.close()

# -----------------------------
# 7-2) Saving results
# -----------------------------
def safe_id(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def save_meta(run_dir: str, items: List[DiscoveredItem]) -> None:
    os.makedirs(run_dir, exist_ok=True)
    csv_path = os.path.join(run_dir, "discovered.csv")
    json_path = os.path.join(run_dir, "discovered.json")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(items[0]).keys()) if items else ["url"])
        w.writeheader()
        for it in items:
            w.writerow(asdict(it))

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump([asdict(it) for it in items], f, ensure_ascii=False, indent=2)

    print(f"[save] meta -> {csv_path}")
    print(f"[save] meta -> {json_path}")


def save_contents(run_dir: str, items: List[DiscoveredItem], max_fetch: int = 100, sleep_s: float = 0.2) -> None:
    """
    글마다 파일 생성하지 않고,
    run_dir 아래에 posts.json 파일 '하나'에 전부 저장.
    """
    os.makedirs(run_dir, exist_ok=True)

    posts: List[Dict] = []
    failures: List[Dict] = []

    for idx, it in enumerate(items[:max_fetch], start=1):
        try:
            post = fetch_post_content(it.url)
            post_record = {
                "url": it.url,
                "postdate": it.postdate,               # YYYYMMDD (API 제공)
                "discovered_at": it.discovered_at,     # ISO
                "method": post.get("method"),
                "final_url": post.get("final_url"),
                "title": post.get("title") or it.title,
                "content_text": post.get("content_text"),
                "fallback_reason": post.get("fallback_reason"),
            }
            posts.append(post_record)
        except Exception as e:
            failures.append({"url": it.url, "error": str(e)})

        time.sleep(sleep_s)

    out_path = os.path.join(run_dir, "posts.json")
    payload = {
        "run_dir": run_dir,
        "saved_at": datetime.now(KST).isoformat(),
        "requested_max_fetch": max_fetch,
        "saved_count": len(posts),
        "failed_count": len(failures),
        "posts": posts,
        "failures": failures,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"[content] saved={len(posts)}, failed={len(failures)}, file={out_path}")


# -----------------------------
# 8) Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", type=int, default=1000, help="목표 unique URL 수(권장 300~1000)")
    ap.add_argument("--display", type=int, default=100, help="API display (max 100)")
    ap.add_argument("--starts", type=str, default="1,101,201", help="start 목록(콤마). 예: 1,101,201")
    ap.add_argument("--tokens", type=int, default=80, help="이번 실행에서 사용할 토큰 개수")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sleep", type=float, default=0.1, help="API 호출 간 sleep(초)")
    ap.add_argument("--fetch-content", action="store_true", help="본문까지 저장")
    ap.add_argument("--max-fetch", type=int, default=200, help="본문 저장 최대 개수(기본 200, 필요 시 1000)")
    ap.add_argument("--out-dir", type=str, default=DATA_DIR, help="결과 저장 루트 디렉토리 (예: /data)")
    ap.add_argument("--seen-db", type=str, default="", help="중복 제거용 sqlite 경로. 예: /data/seen_urls.sqlite")
    ap.add_argument("--seen-ttl-hours", type=int, default=24, help="seen TTL 시간(기본 24시간)")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    client_id = os.getenv("NAVER_CLIENT_ID", "").strip()
    client_secret = os.getenv("NAVER_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise SystemExit("환경변수 NAVER_CLIENT_ID, NAVER_CLIENT_SECRET 를 설정해 주세요.")

    starts = [int(x.strip()) for x in args.starts.split(",") if x.strip()]
    run_dir = os.path.join(args.out_dir, f"run_{datetime.now(KST).strftime('%Y%m%d_%H%M%S')}")

    items = discover_latest(
        client_id=client_id,
        client_secret=client_secret,
        target_unique=args.target,
        display=min(args.display, 100),
        starts=starts,
        tokens_per_run=args.tokens,
        seed=args.seed,
        sleep_s=args.sleep,
    )

    # -----------------------------
    # (NEW) run 간 dedupe: 최근 24시간 seen 스킵
    # -----------------------------
    now_iso = datetime.now(KST).isoformat()
    seen_db = args.seen_db.strip()
    ttl_hours = args.seen_ttl_hours

    if seen_db:
        urls = [it.url for it in items]
        new_urls, skipped = _seen_filter_new(
            urls, seen_db_path=seen_db, ttl_hours=ttl_hours, now_iso=now_iso
        )
        new_set = set(new_urls)
        items = [it for it in items if it.url in new_set]
        print(f"[dedupe] seen_db={seen_db} ttl_hours={ttl_hours} skipped={skipped} remaining={len(items)}")

        if not items:
            print("[dedupe] 신규 URL이 없습니다(최근 TTL 내 중복). 다음 스케줄에서 재시도.")
            return
        
    if not items:
        print("발견된 URL이 없습니다. tokens/starts를 늘리거나 토큰 풀을 바꿔보세요.")
        return

    save_meta(run_dir, items)

    if args.fetch_content:
        save_contents(run_dir, items, max_fetch=args.max_fetch, sleep_s=0.2)

    # -----------------------------
    # seen 마킹: 이번 run에서 처리한 URL 기록
    # -----------------------------
    if args.seen_db:
        _seen_mark([it.url for it in items[:args.max_fetch]], seen_db_path=args.seen_db, now_iso=now_iso)


if __name__ == "__main__":
    main()
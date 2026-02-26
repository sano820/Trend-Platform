# backend/api_server.py
import os
import json
from typing import Optional, Any, Dict, List

from datetime import datetime, date
from dotenv import load_dotenv
load_dotenv()

import redis
import pymysql
from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# -------------------------
# Config (env)
# -------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")  # docker compose 내부면 redis, 로컬이면 localhost
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Flink 실시간 대시보드 키
REDIS_KEY_TREND = os.getenv("REDIS_KEY_TREND", "trend:traffic:10m")
REDIS_KEY_TOP = os.getenv("REDIS_KEY_TOP", "trend:top_tokens:10m")
REDIS_KEY_RISING = os.getenv("REDIS_KEY_RISING", "trend:rising_tokens:10m")

# MySQL (reports)
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "blog_db")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
REPORT_TABLE = os.getenv("REPORT_TABLE", "daily_reports")


# 개발 단계: CORS 꼬임 방지용 전체 허용
# 배포 시에는 allow_origins를 프론트 도메인으로 제한
ALLOW_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")

# -------------------------
# App
# -------------------------
app = FastAPI(title="Dashboard Backend API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOW_ORIGINS] if ALLOW_ORIGINS != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis client (lazy)
_redis: Optional[redis.Redis] = None

def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            retry_on_timeout=True,
        )
    return _redis

def _get_json(r: redis.Redis, key: str) -> Optional[Any]:
    val = r.get(key)
    if not val:
        return None
    try:
        return json.loads(val)
    except json.JSONDecodeError:
        return None


def _get_mysql_conn() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=3,
        read_timeout=5,
        write_timeout=5,
    )



def _iso_to_epoch(ts: str) -> Optional[int]:
    # Spark에서 isoformat()으로 저장한 문자열이므로 fromisoformat으로 처리 가능
    # 예: "2026-02-02T12:34:56"
    try:
        return int(datetime.fromisoformat(ts).timestamp())
    except Exception:
        return None


def _date_to_iso(d: Any) -> Optional[str]:
    if isinstance(d, date):
        return d.isoformat()
    if isinstance(d, datetime):
        return d.date().isoformat()
    return None


def _dt_to_iso(dt: Any) -> Optional[str]:
    if isinstance(dt, datetime):
        return dt.isoformat()
    return None

# -------------------------
# Routes
# -------------------------
@app.get("/health")
def health() -> Dict[str, Any]:
    """
    서버/Redis 상태 확인용.
    """
    try:
        r = get_redis()
        r.ping()
        return {
            "ok": True,
            "redis": "up",
            "keys": {
                "trend": REDIS_KEY_TREND,
                "top": REDIS_KEY_TOP,
                "rising": REDIS_KEY_RISING,
            },
        }
    except Exception as e:
        return {"ok": False, "redis": "down", "error": str(e)}


@app.get("/api/dashboard/latest")
def latest_dashboard(response: Response, limit: Optional[int] = None) -> Any:
    """
    Flink가 Redis에 저장한 실시간 대시보드 값을 그대로 조립해 반환한다.
    - 값 없으면 204
    - JSON 깨지면 502
    - Redis 연결 실패면 503

    Query Parameters:
    - limit: top/rising 항목 개수 제한
    """
    try:
        r = get_redis()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis init failed: {e}")

    traffic = _get_json(r, REDIS_KEY_TREND)
    top = _get_json(r, REDIS_KEY_TOP)
    rising = _get_json(r, REDIS_KEY_RISING)

    # 아무것도 없으면 204
    if not any([traffic, top, rising]):
        response.status_code = 204
        return None

    def _limit_items(payload: Any) -> Any:
        if not payload or not isinstance(payload, dict):
            return payload
        items = payload.get("items")
        if isinstance(items, list) and limit and limit > 0:
            payload = dict(payload)
            payload["items"] = items[:limit]
        return payload

    return {
        "traffic": traffic,
        "top": _limit_items(top),
        "rising": _limit_items(rising),
    }


@app.get("/api/reports")
def list_reports(limit: int = 30) -> Dict[str, Any]:
    """
    일일 보고서 목록.
    """
    sql = (
        f"SELECT report_date, title, summary, created_at "
        f"FROM {REPORT_TABLE} "
        "ORDER BY report_date DESC "
        "LIMIT %s"
    )
    try:
        with _get_mysql_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"report query failed: {e}")

    items: List[Dict[str, Any]] = []
    for r in rows:
        items.append(
            {
                "report_date": _date_to_iso(r.get("report_date")),
                "title": r.get("title"),
                "summary": r.get("summary"),
                "created_at": _dt_to_iso(r.get("created_at")),
            }
        )
    return {"items": items}


@app.get("/api/reports/{report_date}")
def get_report(report_date: str) -> Dict[str, Any]:
    """
    특정 날짜의 보고서 상세.
    """
    sql = (
        f"SELECT report_date, title, summary, content_md, keywords_json, created_at "
        f"FROM {REPORT_TABLE} "
        "WHERE report_date = %s "
        "LIMIT 1"
    )
    try:
        with _get_mysql_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (report_date,))
                row = cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"report query failed: {e}")

    if not row:
        raise HTTPException(status_code=404, detail="report not found")

    keywords_raw = row.get("keywords_json")
    keywords = None
    if isinstance(keywords_raw, str):
        try:
            keywords = json.loads(keywords_raw)
        except json.JSONDecodeError:
            keywords = None

    return {
        "report_date": _date_to_iso(row.get("report_date")),
        "title": row.get("title"),
        "summary": row.get("summary"),
        "content_md": row.get("content_md"),
        "keywords": keywords,
        "created_at": _dt_to_iso(row.get("created_at")),
    }

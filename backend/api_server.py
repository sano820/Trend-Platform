# backend/api_server.py
import os
import json
from typing import Optional, Any, Dict

from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

import redis
from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# -------------------------
# Config (env)
# -------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")  # docker compose 내부면 redis, 로컬이면 localhost
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KEY_CONTENT = os.getenv("KEY_CONTENT", "metrics:content_change_ratio_1m")
CONTENT_WINDOW_SEC = int(os.getenv("CONTENT_WINDOW_SEC", "60"))


# spark가 실제로 쓴느 키들
KEY_TOTAL = os.getenv("KEY_TOTAL", "metrics:events_total_10m_10s")
KEY_BY_TYPE = os.getenv("KEY_BY_TYPE", "metrics:events_by_type_10m_10s")
KEY_BOT = os.getenv("KEY_BOT", "metrics:bot_ratio_1m")
KEY_TOP = os.getenv("KEY_TOP", "metrics:top_domain_5m_top10")

# 프론트에서 기대하는 meta 값(고정해도 됨)
BUCKET_SEC = int(os.getenv("BUCKET_SEC", "10"))
RANGE_MIN = int(os.getenv("RANGE_MIN", "10"))
TOP_WINDOW_MIN = int(os.getenv("TOP_WINDOW_MIN", "5"))
BOT_WINDOW_SEC = int(os.getenv("BOT_WINDOW_SEC", "60"))


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




def _iso_to_epoch(ts: str) -> Optional[int]:
    # Spark에서 isoformat()으로 저장한 문자열이므로 fromisoformat으로 처리 가능
    # 예: "2026-02-02T12:34:56"
    try:
        return int(datetime.fromisoformat(ts).timestamp())
    except Exception:
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
                "total": KEY_TOTAL,
                "byType": KEY_BY_TYPE,
                "bot": KEY_BOT,
                "top": KEY_TOP,
                "content": KEY_CONTENT,
            },
        }
    except Exception as e:
        return {"ok": False, "redis": "down", "error": str(e)}


@app.get("/api/dashboard/latest")
def latest_dashboard(response: Response, limit: Optional[int] = None) -> Any:
    """
    Redis의 REDIS_KEY 값을 JSON으로 파싱해서 반환.
    - 값 없으면 204
    - JSON 깨지면 502
    - Redis 연결 실패면 503

    Spark가 Redis에 쌓아둔 4개 메트릭 키를 읽어서
    프론트가 원하는 dashboard JSON 형태로 '조립'해서 반환한다.

    Query Parameters:
    - limit: 반환할 데이터 포인트 개수 제한 (기본값: 제한 없음, 전체 반환)
    """
    try:
        r = get_redis()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis init failed: {e}")

    total_raw = _get_json(r, KEY_TOTAL)        # list[{ts,count}]
    by_type_raw = _get_json(r, KEY_BY_TYPE)    # list[{ts,type_counts:{...}}]
    bot_raw = _get_json(r, KEY_BOT)            # {ts, bot_ratio, bot_count, total_count}
    top_raw = _get_json(r, KEY_TOP)            # list[{domain,count}]
    content_raw = _get_json(r, KEY_CONTENT)  # {ts, content_change_ratio, content_changed_count, total_revision_count}

    # 아무것도 없으면 204
    if not any([total_raw, by_type_raw, bot_raw, top_raw, content_raw]):
        response.status_code = 204
        return None

    now_epoch = int(datetime.now().timestamp())

    # (1) total: [{t,v}]
    total = []
    if isinstance(total_raw, list):
        for item in total_raw:
            ts = item.get("ts")
            ep = _iso_to_epoch(ts) if ts else None
            if ep is None:
                continue
            total.append({"t": ep, "v": int(item.get("count", 0))})
        # 시간 오름차순 정렬
        total.sort(key=lambda x: x["t"])
        # limit 적용 (최신 데이터부터)
        if limit and limit > 0:
            total = total[-limit:]

    # (2) byType: {type: [{t,v}]}
    byType: Dict[str, list] = {}
    if isinstance(by_type_raw, list):
        for win in by_type_raw:
            ts = win.get("ts")
            ep = _iso_to_epoch(ts) if ts else None
            if ep is None:
                continue
            counts = win.get("type_counts") or {}
            if isinstance(counts, dict):
                for tname, cnt in counts.items():
                    byType.setdefault(tname, []).append({"t": ep, "v": int(cnt)})

        # 각 타입별 시간 정렬 및 limit 적용
        for k in list(byType.keys()):
            byType[k].sort(key=lambda x: x["t"])
            if limit and limit > 0:
                byType[k] = byType[k][-limit:]

    # (3) bot: {windowSec,total,bot,ratio}
    bot = {"windowSec": BOT_WINDOW_SEC, "total": 0, "bot": 0, "ratio": 0.0}
    if isinstance(bot_raw, dict):
        bot["total"] = int(bot_raw.get("total_count", 0))
        bot["bot"] = int(bot_raw.get("bot_count", 0))
        bot["ratio"] = float(bot_raw.get("bot_ratio", 0.0))

    # (4) topWiki: [{k,v}]  (Spark는 domain을 쓰고 있으니 domain을 k로 매핑)
    topWiki = []
    if isinstance(top_raw, list):
        for item in top_raw:
            topWiki.append({"k": item.get("domain"), "v": int(item.get("count", 0))})

    # (5) 
    contentChange = {"windowSec": CONTENT_WINDOW_SEC, "total": 0, "changed": 0, "ratio": 0.0}
    if isinstance(content_raw, dict):
        contentChange["total"] = int(content_raw.get("total_revision_count", 0))
        contentChange["changed"] = int(content_raw.get("content_changed_count", 0))
        contentChange["ratio"] = float(content_raw.get("content_change_ratio", 0.0))
        

    dashboard = {
        "bucketSec": BUCKET_SEC,
        "rangeMin": RANGE_MIN,
        "now": now_epoch,
        "total": total,
        "byType": byType,
        "bot": bot,
        "topWiki": topWiki,
        "contentChange": contentChange,   # ✅ 추가
    }
    return dashboard
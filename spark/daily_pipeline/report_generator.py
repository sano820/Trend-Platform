from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime
from typing import Any, Dict, List

import pymysql
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate daily trend report using OpenAI API")
    p.add_argument("--report-date", help="Report date (YYYY-MM-DD). Defaults to today.")
    p.add_argument("--top-n", type=int, default=int(os.getenv("REPORT_TOP_N", "20")))
    return p.parse_args()


def _parse_report_date(s: str | None) -> date:
    if not s:
        return date.today()
    s = s.strip()
    if "-" in s:
        return datetime.strptime(s, "%Y-%m-%d").date()
    return datetime.strptime(s, "%Y%m%d").date()


def _mysql_conn() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
        database=os.getenv("MYSQL_DATABASE", "blog_db"),
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=3,
        read_timeout=10,
        write_timeout=10,
    )


def _fetch_predictions(report_date: date, top_n: int) -> List[Dict[str, Any]]:
    pred_table = os.getenv("PRED_TABLE", "predictions")
    sql = (
        f"SELECT token, predicted_count, score, status, burst, today_count, yest_count, "
        f"avg_3d_count, avg_7d_count, doc_today, doc_yest "
        f"FROM {pred_table} "
        "WHERE predict_date = %s "
        "ORDER BY score DESC "
        "LIMIT %s"
    )
    with _mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (report_date.isoformat(), top_n))
            return cur.fetchall()


def _build_prompt(report_date: date, rows: List[Dict[str, Any]], top_n: int) -> str:
    payload = {
        "report_date": report_date.isoformat(),
        "top_n": top_n,
        "items": rows,
    }

    return (
        "너는 한국어로 데이터 분석 리포트를 작성하는 애널리스트다. "
        "아래 JSON 데이터를 바탕으로 '일일 트렌드 리포트'를 작성해라. "
        "반드시 JSON만 출력하고, 다음 스키마를 따라라.\n\n"
        "{\n"
        "  \"title\": string,\n"
        "  \"summary\": string,\n"
        "  \"content_md\": string (Markdown),\n"
        "  \"keywords\": string[]\n"
        "}\n\n"
        "제약:\n"
        "- content_md에는 섹션 제목을 ## 로 표기한다.\n"
        "- keywords는 상위 핵심 키워드 5~10개로 구성한다.\n"
        "- 데이터가 부족하면 과장하지 말고 '데이터가 제한적'이라고 명시한다.\n\n"
        f"DATA:\n{json.dumps(payload, ensure_ascii=False)}"
    )


def _call_openai(prompt: str) -> Dict[str, Any]:
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    client = OpenAI()

    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": "You are a careful data analyst. Return JSON only.",
            },
            {
                "role": "user",
                "content": prompt,
            },
        ],
        text={"format": {"type": "json_object"}},
        temperature=float(os.getenv("OPENAI_TEMPERATURE", "0.4")),
    )

    try:
        return json.loads(response.output_text)
    except json.JSONDecodeError:
        raise RuntimeError("OpenAI response was not valid JSON")


def _fallback_report(report_date: date, rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    keywords = [r.get("token") for r in rows[:10] if r.get("token")]
    lines = [
        f"## {report_date.isoformat()} 일일 트렌드 요약",
        "- 데이터가 제한적이어서 간단 요약만 제공됩니다.",
        "- 상위 키워드를 참고해주세요.",
        "",
        "## Top Keywords",
    ]
    for i, r in enumerate(rows[:10], start=1):
        lines.append(f"- {i}. {r.get('token')} (score {r.get('score')})")

    return {
        "title": f"{report_date.isoformat()} 데일리 트렌드 리포트",
        "summary": "예측 데이터 기준으로 상위 키워드를 요약했습니다.",
        "content_md": "\n".join(lines),
        "keywords": keywords,
    }


def _save_report(report_date: date, report: Dict[str, Any]) -> None:
    report_table = os.getenv("REPORT_TABLE", "daily_reports")
    sql = (
        f"INSERT INTO {report_table} "
        "(report_date, title, summary, content_md, keywords_json) "
        "VALUES (%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE "
        "title=VALUES(title), summary=VALUES(summary), content_md=VALUES(content_md), keywords_json=VALUES(keywords_json)"
    )

    keywords_json = json.dumps(report.get("keywords") or [], ensure_ascii=False)

    with _mysql_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    report_date.isoformat(),
                    report.get("title"),
                    report.get("summary"),
                    report.get("content_md"),
                    keywords_json,
                ),
            )
        conn.commit()


def main() -> None:
    args = parse_args()
    report_date = _parse_report_date(args.report_date)

    rows = _fetch_predictions(report_date, args.top_n)
    if not rows:
        raise RuntimeError(f"No prediction rows for {report_date.isoformat()}")

    prompt = _build_prompt(report_date, rows, args.top_n)

    try:
        report = _call_openai(prompt)
    except Exception:
        report = _fallback_report(report_date, rows)

    _save_report(report_date, report)
    print(f"report saved for {report_date.isoformat()}")


if __name__ == "__main__":
    main()

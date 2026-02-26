from __future__ import annotations

import pymysql

from config import MysqlConfig


def run_sql(mysql: MysqlConfig, sql: str, args: tuple | None = None) -> None:
    """단일 SQL 실행용 유틸(autocommit)."""
    conn = pymysql.connect(
        host=mysql.host,
        port=mysql.port,
        user=mysql.user,
        password=mysql.password,
        database=mysql.database,
        charset="utf8mb4",   # 한글/이모지 깨짐 방지
        autocommit=True,     # 배치에서 명시적 commit 생략(바로 반영)
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)  # args는 SQL 인젝션 방지를 위해 파라미터 바인딩으로 전달
    finally:
        conn.close()


def run_many(mysql: MysqlConfig, sql: str, rows: list[tuple]) -> None:
    """여러 행을 한 번에 넣을 때(executemany) 사용하는 유틸."""
    if not rows:
        return  # 넣을 데이터가 없으면 DB 연결도 안 함

    conn = pymysql.connect(
        host=mysql.host,
        port=mysql.port,
        user=mysql.user,
        password=mysql.password,
        database=mysql.database,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)  # rows는 (컬럼순서대로 값...) 튜플 리스트
    finally:
        conn.close()


def delete_by_date(mysql: MysqlConfig, table: str, date_col: str, date_str_yyyy_mm_dd: str) -> None:
    """멱등성 보장을 위해, 특정 날짜(date_col=YYYY-MM-DD) 데이터만 먼저 삭제."""
    run_sql(mysql, f"DELETE FROM {table} WHERE {date_col} = %s", (date_str_yyyy_mm_dd,))
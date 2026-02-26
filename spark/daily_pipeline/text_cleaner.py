from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F


def build_cleaned_text(title_col: Column, content_col: Column) -> Column:
    """
    분석용 정제 텍스트 생성 = (제목 + 본문) 결합 후 노이즈 제거.

    필수 규칙:
    1) 제목 + 본문을 공백으로 합치기
    2) URL 제거
    3) 대부분의 특수문자 제거(한글/영문/숫자/공백만 남기기)
    4) 공백 정규화(연속 공백 -> 단일 공백) + 앞뒤 공백 제거
    """

    # 제목/본문이 NULL일 수 있으니 빈 문자열로 대체한 뒤 합침
    raw = F.concat_ws(" ", F.coalesce(title_col, F.lit("")), F.coalesce(content_col, F.lit("")))

    # URL 제거 (http:// 또는 https:// 로 시작하는 링크 제거)
    no_url = F.regexp_replace(raw, r"https?://\\S+", " ")

    # 한글/영문/숫자/공백만 남기고 나머지는 공백으로 치환
    no_special = F.regexp_replace(no_url, r"[^0-9A-Za-z가-힣\\s]", " ")

    # 연속 공백을 하나로 줄이고, 문자열 양끝 공백 제거
    normalized = F.trim(F.regexp_replace(no_special, r"\\s+", " "))

    return normalized
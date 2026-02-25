from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Iterable

from pyspark.sql import functions as F
from pyspark.sql import types as T


_RE_ONLY_NUMBER = re.compile(r"^\d+$")         # 숫자만 있는 토큰 필터(예: "123")
_RE_LAUGH_OR_CRY = re.compile(r"^[ㅋㅎㅠㅜ]+$") # 웃음/울음 반복 토큰 필터(예: "ㅋㅋㅋ", "ㅠㅠ")
_RE_HAS_KO_EN_NUM = re.compile(r"[0-9A-Za-z가-힣]")  # 한글/영문/숫자 중 하나라도 포함하는지 체크

# 토크나이저 모드 안내:
# - TOKENIZER_MODE=morph (기본): 형태소 분석 기반(명사/외래어 중심)
# - TOKENIZER_MODE=split        : 공백 split(가벼운 베이스라인)
#
# 형태소 분석은 kiwipiepy(Kiwi)를 사용한다.
# -> Spark 워커(Executor)에서도 import 되어야 하므로, Spark 이미지 requirements에 kiwipiepy가 포함되어 있어야 한다.

_KIWI = None  # 워커 프로세스별로 Kiwi 인스턴스를 1번만 생성해서 재사용(성능/안정성)


def load_stopwords(stopwords_path: str) -> set[str]:
    """불용어(stopwords) 파일을 읽어서 set으로 반환한다. (한 줄에 한 단어)"""
    p = Path(stopwords_path)
    if not p.exists():
        return set()

    words: set[str] = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        w = line.strip()
        # 빈 줄/주석(#)은 무시
        if not w or w.startswith("#"):
            continue
        words.add(w)
    return words


def _filter_token(token: str, *, stopwords: set[str], min_len: int, max_len: int) -> str | None:
    """
    토큰 품질 필터.
    - 길이/숫자-only/ㅋㅋㅋ/특수문자-only/불용어 등을 제거한다.
    - 최종적으로 통과한 토큰은 소문자 변환 후 반환한다.
    """
    if not token:
        return None

    t = token.strip().lower()
    if not t:
        return None

    # 길이 제한
    if len(t) < min_len:
        return None
    if len(t) > max_len:
        return None

    # 숫자만 있는 경우 제거
    if _RE_ONLY_NUMBER.match(t):
        return None

    # "ㅋㅋㅋ", "ㅠㅠ" 같은 의미 없는 반복 토큰 제거
    if _RE_LAUGH_OR_CRY.match(t):
        return None

    # 한글/영문/숫자가 하나도 없으면 제거(이모지/기호만 있는 토큰 방지)
    if not _RE_HAS_KO_EN_NUM.search(t):
        return None

    # 불용어 제거
    if t in stopwords:
        return None

    return t


def _parse_allowed_tags(raw: str) -> set[str]:
    """환경변수 MORPH_ALLOWED_TAGS 문자열을 set으로 파싱한다. (예: 'NNG,NNP,SL')"""
    tags = [t.strip() for t in raw.split(",")]
    return {t for t in tags if t}


def _get_kiwi():
    """
    워커(executor) 파이썬 프로세스에서 Kiwi 인스턴스를 '지연 생성(lazy)' 후 재사용한다.
    - UDF는 워커에서 실행되므로, kiwipiepy가 워커 환경에도 설치되어 있어야 한다.
    """
    global _KIWI
    if _KIWI is None:
        try:
            from kiwipiepy import Kiwi
        except ImportError as e:
            raise RuntimeError(
                "TOKENIZER_MODE=morph는 'kiwipiepy'가 필요합니다. "
                "Spark 이미지(requirements.txt)에 설치하거나 TOKENIZER_MODE=split로 변경하세요."
            ) from e
        _KIWI = Kiwi()
    return _KIWI


def _tokenize_split(text: str) -> list[str]:
    """공백 split 기반 토큰화(베이스라인)."""
    return text.split()


def _tokenize_morph(text: str, allowed_tags: set[str]) -> list[str]:
    """
    형태소 분석 기반 토큰화(Kiwi).
    - Kiwi 결과에서 form(표면형), tag(POS)를 보고
    - allowed_tags에 포함된 품사만 추출한다. (기본: 명사류 + 외래어(SL))
    """
    kiwi = _get_kiwi()

    out: list[str] = []
    for t in kiwi.tokenize(text):
        if t.tag in allowed_tags:
            out.append(t.form)
    return out


def make_tokenize_udf(stopwords: set[str], min_len: int, max_len: int):
    """
    UDF 생성: cleaned_text -> tokens(list)
    - tokens_all: 문서 내 중복 포함(mention_count 계산에 사용)

    관련 환경변수:
      - TOKENIZER_MODE: morph | split (기본: morph)
      - MORPH_ALLOWED_TAGS: 허용 품사 태그 목록(기본: NNG,NNP,NNB,NR,NP,SL)

    집계 규칙 참고:
      - mention_count = explode(tokens_all) => 문서 내 반복 포함(언급량)
      - doc_count     = explode(tokens_set) => 문서 내 중복 제거(등장 문서 수)
    """
    mode = os.getenv("TOKENIZER_MODE", "morph").strip().lower()
    allowed_tags = _parse_allowed_tags(os.getenv("MORPH_ALLOWED_TAGS", "NNG,NNP,NNB,NR,NP,SL"))

    @F.udf(returnType=T.ArrayType(T.StringType()))
    def _tokenize(text: str | None) -> list[str]:
        # NULL/빈 문자열 방어
        if not text:
            return []

        # 토큰화(모드에 따라 split 또는 morph)
        if mode == "split":
            raw_tokens = _tokenize_split(text)
        else:
            raw_tokens = _tokenize_morph(text, allowed_tags)

        # 토큰 품질 필터 적용
        out: list[str] = []
        for tok in raw_tokens:
            ft = _filter_token(tok, stopwords=stopwords, min_len=min_len, max_len=max_len)
            if ft is not None:
                out.append(ft)
        return out

    return _tokenize


def make_distinct_udf():
    """
    UDF 생성: tokens(list) -> unique tokens(list)
    - 한 문서 안에서 같은 토큰이 여러 번 나와도 1번만 남기는 용도
    - doc_count(등장 문서 수) 집계에 사용
    """

    @F.udf(returnType=T.ArrayType(T.StringType()))
    def _distinct(tokens: Iterable[str] | None) -> list[str]:
        if not tokens:
            return []
        seen: set[str] = set()
        out: list[str] = []
        for t in tokens:
            if t not in seen:
                seen.add(t)
                out.append(t)
        return out

    return _distinct
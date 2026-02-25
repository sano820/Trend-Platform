from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class MysqlConfig:
    """
    MySQL 접속 설정을 담는 설정 클래스.

    - docker-compose 환경/로컬 환경 모두 대응하기 위해
      값은 환경변수(.env)에서 읽고, 없으면 기본값을 사용한다.
    - frozen=True: 런타임 중 설정이 실수로 바뀌지 않도록 '불변(immutable)'로 고정
    """
    host: str          # MySQL 호스트명 (예: docker-compose 내부면 'mysql', 외부면 IP/도메인)
    port: int          # MySQL 포트 (기본 3306)
    database: str      # 사용할 DB 이름 (예: blog_db)
    user: str          # MySQL 유저
    password: str      # MySQL 비밀번호

    @property
    def jdbc_url(self) -> str:
        """
        Spark JDBC로 MySQL에 접속할 때 사용하는 JDBC URL.

        - useUnicode/characterEncoding: 한글 깨짐 방지
        - serverTimezone: 날짜/시간 처리 시 타임존 꼬임 방지
          (특히 DATE/DATETIME 컬럼 읽을 때 하루 밀리는 이슈를 줄이는 데 도움)
        """
        return (
            f"jdbc:mysql://{self.host}:{self.port}/{self.database}"
            f"?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Seoul"
        )


@dataclass(frozen=True)
class PipelineConfig:
    """
    배치 예측 파이프라인 전체 동작을 제어하는 설정 클래스.

    Spark 배치 실행 시, 아래 값들이 기준이 되어:
    - 최근 window_days(기본 7일) 데이터로
    - 토큰 후보를 필터링하고
    - 안정형/급상승형/불확실을 분류한 뒤
    - 내일(D+1) 예측 및 Top-N 랭킹을 만든다.
    """
    run_date: date     # "오늘"로 간주할 기준 날짜 (D). 예측은 D+1로 저장됨
    window_days: int   # 최근 며칠을 윈도우로 볼지 (기본 7)
    top_n: int         # 최종 결과 Top-N (기본 50)

    # ---------------------------
    # 후보 토큰 필터(오늘 기준 최소 활동량)
    # ---------------------------
    min_doc_today: int    # 오늘 등장 문서 수 최소 기준(예: 5)
    min_count_today: int  # 오늘 등장 횟수 최소 기준(예: 10)

    # ---------------------------
    # 급상승(버스트) 분류 임계치
    # ---------------------------
    growth_ratio_min: float  # (오늘+1)/(어제+1) 성장비율 최소 기준(예: 2.0)
    prev_mean_max: float     # 최근 7일 중 오늘 제외 평균이 이 값 이하이면 "원래 없던 키워드"로 판단(예: 3.0)

    # ---------------------------
    # 랭킹 점수 가중치(상태별 부스팅)
    # ---------------------------
    w_burst: float       # 급상승형 버스트 가중치(예: 1.5)
    w_stable: float      # 안정형 가중치(예: 0.7)
    w_uncertain: float   # 불확실 가중치(예: 0.3)

    # ---------------------------
    # 폭주 방지(예측값 캡)
    # ---------------------------
    yhat_cap_multiplier: float
    # 예: 3.0이면 "내일 예측 카운트 ŷ <= 3 * 오늘 카운트"로 제한
    # 짧은 윈도우(7일)에서 스파이크가 과대평가되는 걸 방지

    # ---------------------------
    # 재계산 옵션
    # ---------------------------
    rebuild_window: bool
    # True면: run_date 기준 window_days 범위를 통째로 다시 계산/업서트(재처리)
    # False면: 주로 run_date 하루치만 처리하는 형태로 운용 가능

    # ---------------------------
    # 토큰 품질 필터(형태/길이 기준)
    # ---------------------------
    min_token_len: int   # 최소 토큰 길이(예: 2)
    max_token_len: int   # 최대 토큰 길이(예: 20)


def load_mysql_config_from_env() -> MysqlConfig:
    """
    환경변수에서 MySQL 접속 정보를 읽어서 MysqlConfig를 만든다.

    우선순위:
    1) docker-compose/.env에 설정한 값
    2) 없으면 코드에 지정된 기본값

    기본값 의미:
    - host="mysql": docker-compose 내부 네트워크에서 mysql 서비스 이름으로 접속하는 기본 패턴
    - user/password="trend": 로컬 테스트용 기본값(실제 프로젝트 값에 맞춰 env로 덮어쓰는 것을 권장)
    """
    return MysqlConfig(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DATABASE", "blog_db"),
        user=os.getenv("MYSQL_USER", "trend"),
        password=os.getenv("MYSQL_PASSWORD", "trend"),
    )
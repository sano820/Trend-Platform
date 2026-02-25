from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge


@dataclass(frozen=True)
class PredictionParams:
    # 최근 N일 윈도우(기본 7일)만 보고 D+1(내일) 예측을 수행
    window_days: int = 7
    # 최종 출력 Top-N
    top_n: int = 50

    # ---------------------------
    # 후보 토큰 필터(오늘 기준 최소 활동량)
    # ---------------------------
    min_doc_today: int = 5      # 오늘 등장 문서 수 최소 기준
    min_count_today: int = 10   # 오늘 등장 횟수 최소 기준

    # ---------------------------
    # 상태 분류(안정형/급상승형) 임계치
    # ---------------------------
    growth_ratio_min: float = 2.0  # 성장비율(오늘+1)/(어제+1) 최소 기준
    prev_mean_max: float = 3.0     # 과거 평균(오늘 제외)이 이 값 이하이면 "원래 없던 키워드"로 판단

    # ---------------------------
    # 점수(랭킹) 가중치(상태별 부스팅)
    # ---------------------------
    w_burst: float = 1.5
    w_stable: float = 0.7
    w_uncertain: float = 0.3

    # ---------------------------
    # 폭주 방지 캡(예측값 상한)
    # ---------------------------
    yhat_cap_multiplier: float = 3.0  # 예: 3.0이면 ŷ <= 3 * 오늘카운트


def _sigmoid(x: float) -> float:
    """시그모이드 함수(0~1). burst 점수를 점수 계산에 부드럽게 반영하기 위해 사용."""
    # overflow 방지(큰 값 exp 계산 시 터지는 것 방지)
    if x >= 0:
        z = math.exp(-x)
        return 1 / (1 + z)
    z = math.exp(x)
    return z / (1 + z)


def _clip(x: float, lo: float, hi: float) -> float:
    """값을 [lo, hi] 범위로 자르는 유틸."""
    return max(lo, min(hi, x))


def _build_training_rows(row: pd.Series) -> tuple[list[list[float]], list[float]]:
    """
    한 토큰의 7일 시계열(카운트/문서수)을 이용해 학습 데이터(X, y_log1p)를 만든다.

    - counts/docs 배열은 오래된 날짜 -> 최신 날짜 순서: (D-6 .. D)
    - 7일로 만들 수 있는 "전이(transition)" 샘플은 6개(j=0..5)
      * 입력: t 시점의 특징(최근값/이동평균/모멘텀/문서기반 특징 등)
      * 타깃: t+1 시점의 카운트
    - 타깃은 log1p 변환해서 Ridge 회귀가 안정적으로 학습되도록 함
    """
    c: np.ndarray = row["counts"].astype(float)
    d: np.ndarray = row["docs"].astype(float)

    X: list[list[float]] = []
    y: list[float] = []

    for j in range(0, 6):
        # ---------------------------
        # lag feature 생성
        # ---------------------------
        # x = [count_lag0..6, doc_lag0..6] 형태
        # 여기서 lag0는 현재(학습샘플 기준 t=j), lag1은 t-1 ...
        x = []
        for k in range(0, 7):
            idx = j - k
            x.append(float(c[idx]) if idx >= 0 else 0.0)
        for k in range(0, 7):
            idx = j - k
            x.append(float(d[idx]) if idx >= 0 else 0.0)

        # ---------------------------
        # 파생 특징(이동평균/모멘텀/변동성/문서기반 안정화)
        # ---------------------------
        x0, x1, x2 = x[0], x[1], x[2]
        ma3 = np.mean([x0, x1, x2])                          # 최근 3일 평균(가능 구간)
        ma7 = np.mean([float(c[i]) for i in range(0, j + 1)])  # 학습 시점까지의 평균(가용 구간)
        vol7 = float(np.std([float(c[i]) for i in range(0, j + 1)], ddof=0))  # 변동성

        doc0, doc1 = x[7 + 0], x[7 + 1]
        doc_ma3 = np.mean([doc0, doc1, x[7 + 2]])            # 최근 3일 문서 수 평균
        doc_growth = math.log((doc0 + 1.0) / (doc1 + 1.0))    # 문서 수 성장률(log)
        repeat_ratio = x0 / (doc0 + 1.0)                      # 한 문서 반복(스팸성) 감지

        mom1 = x0 - x1  # 1일 모멘텀
        mom2 = x1 - x2  # 2일 모멘텀(추세 방향 보조)

        # 최종 feature 벡터에 추가
        x.extend([ma3, ma7, mom1, mom2, vol7, doc0, doc_ma3, doc_growth, repeat_ratio])

        # 타깃: 다음날 카운트(t+1)
        target = float(c[j + 1])
        X.append(x)
        y.append(math.log1p(target))  # 로그 변환(카운트 안정화)

    return X, y


def _build_infer_features(row: pd.Series) -> list[float]:
    """예측(inference) 시점(D)의 feature 벡터를 만든다."""
    c: np.ndarray = row["counts"].astype(float)
    d: np.ndarray = row["docs"].astype(float)

    # 오늘(today)은 7일 중 최신 인덱스(6)
    j = 6

    # lag feature 구성(오늘 기준)
    x = []
    for k in range(0, 7):
        idx = j - k
        x.append(float(c[idx]) if idx >= 0 else 0.0)
    for k in range(0, 7):
        idx = j - k
        x.append(float(d[idx]) if idx >= 0 else 0.0)

    # 파생 특징(오늘 기준으로 계산)
    x0, x1, x2 = x[0], x[1], x[2]
    ma3 = np.mean([x0, x1, x2])        # 최근 3일 평균
    ma7 = float(np.mean(c))            # 최근 7일 평균
    vol7 = float(np.std(c, ddof=0))    # 최근 7일 변동성

    doc0, doc1 = x[7 + 0], x[7 + 1]
    doc_ma3 = np.mean([doc0, doc1, x[7 + 2]])
    doc_growth = math.log((doc0 + 1.0) / (doc1 + 1.0))
    repeat_ratio = x0 / (doc0 + 1.0)

    mom1 = x0 - x1
    mom2 = x1 - x2

    x.extend([ma3, ma7, mom1, mom2, vol7, doc0, doc_ma3, doc_growth, repeat_ratio])
    return x


def predict_next_day(
    *,
    daily_stats: pd.DataFrame,
    run_date: date,
    params: PredictionParams,
) -> pd.DataFrame:
    """
    D(오늘) 기준으로, D+1(내일) 예측 결과 Top-N을 반환한다.

    입력 daily_stats 컬럼:
      - stat_date (date 또는 str)
      - token (str)
      - mention_count (int)  : 문서 내 반복 포함 카운트
      - doc_count (int)      : 문서 기준 등장 수(문서 내 중복 제거)

    전제:
      - 최근 window_days(기본 7일) 데이터만으로 동작하도록 설계됨
    """
    if daily_stats.empty:
        # 입력이 비어있으면 빈 결과 반환(스키마만 맞춰서 반환)
        return pd.DataFrame(columns=[
            "predict_date",
            "token",
            "predicted_count",
            "score",
            "status",
            "burst",
            "today_count",
            "yest_count",
            "avg_3d_count",
            "avg_7d_count",
            "doc_today",
            "doc_yest",
        ])

    # 날짜 타입 정규화(stat_date를 date로 맞춤)
    daily_stats = daily_stats.copy()
    daily_stats["stat_date"] = pd.to_datetime(daily_stats["stat_date"]).dt.date

    # 최근 window_days 날짜 리스트(오래된->최신)
    dates = [run_date - timedelta(days=i) for i in range(params.window_days - 1, -1, -1)]

    # token 기준 wide 형태로 피벗 (없는 날짜는 0으로 채움)
    cwide = (
        daily_stats.pivot_table(index="token", columns="stat_date", values="mention_count", aggfunc="sum")
        .reindex(columns=dates, fill_value=0)
    )
    dwide = (
        daily_stats.pivot_table(index="token", columns="stat_date", values="doc_count", aggfunc="sum")
        .reindex(columns=dates, fill_value=0)
    )

    # token별로 counts(7일 배열), docs(7일 배열) 컬럼으로 묶기
    base = pd.DataFrame(index=cwide.index)
    base["counts"] = list(cwide.to_numpy(dtype=float))
    base["docs"] = list(dwide.to_numpy(dtype=float))

    # 오늘/어제의 핵심 값 추출(오늘=최신)
    base["c0"] = base["counts"].apply(lambda a: int(a[-1]))  # 오늘 카운트
    base["c1"] = base["counts"].apply(lambda a: int(a[-2]))  # 어제 카운트
    base["doc0"] = base["docs"].apply(lambda a: int(a[-1]))  # 오늘 문서 수
    base["doc1"] = base["docs"].apply(lambda a: int(a[-2]))  # 어제 문서 수

    # 후보 토큰 필터(오늘 활동량 기준으로 노이즈 제거)
    base = base[(base["doc0"] >= params.min_doc_today) & (base["c0"] >= params.min_count_today)].copy()
    if base.empty:
        return pd.DataFrame(columns=[
            "predict_date",
            "token",
            "predicted_count",
            "score",
            "status",
            "burst",
            "today_count",
            "yest_count",
            "avg_3d_count",
            "avg_7d_count",
            "doc_today",
            "doc_yest",
        ])

    # -----------------------------
    # 상태 분류: stable / burst / uncertain
    # -----------------------------
    base["non_zero_days"] = base["counts"].apply(lambda a: int(np.sum(np.array(a) > 0)))  # 7일 중 등장한 날 수
    base["prev_mean"] = base["counts"].apply(lambda a: float(np.mean(a[:-1])))           # D-6..D-1 평균(오늘 제외)
    base["growth_ratio"] = (base["c0"] + 1.0) / (base["c1"] + 1.0)                       # 성장비율(+1 스무딩)

    # 안정형: 7일 중 4일 이상 등장
    is_stable = base["non_zero_days"] >= 4

    # 급상승형: 성장비율 높고, 과거 평균은 낮으며, 안정형은 아닌 경우
    is_burst = (
        (base["growth_ratio"] >= params.growth_ratio_min)
        & (base["prev_mean"] <= params.prev_mean_max)
        & (~is_stable)
    )

    base["status"] = np.where(is_stable, "stable", np.where(is_burst, "burst", "uncertain"))

    # -----------------------------
    # 버스트 점수 계산(b1 + b2 혼합)
    # -----------------------------
    def _burst_parts(a: np.ndarray) -> tuple[float, float]:
        prev = np.array(a[:-1], dtype=float)  # D-6..D-1
        mu = float(prev.mean())
        sigma = float(prev.std(ddof=0))
        return mu, sigma

    base[["mu", "sigma"]] = base["counts"].apply(lambda a: pd.Series(_burst_parts(np.array(a, dtype=float))))
    base["b1"] = (base["c0"] - base["mu"]) / (base["sigma"] + 1.0)          # 최근 평균 대비 튄 정도(분모 +1 안정화)
    base["b2"] = np.log((base["c0"] + 1.0) / (base["c1"] + 1.0))            # 어제 대비 성장률(log)
    base["burst"] = 0.7 * base["b2"] + 0.3 * base["b1"].apply(lambda x: _clip(float(x), -3.0, 6.0))

    # 저장/설명용 기본 통계(최근 3일 평균, 최근 7일 평균)
    base["avg_3d_count"] = base["counts"].apply(lambda a: float(np.mean(np.array(a[-3:], dtype=float))))
    base["avg_7d_count"] = base["counts"].apply(lambda a: float(np.mean(np.array(a, dtype=float))))

    # -----------------------------
    # 트랙 1) 안정형(stable) -> Ridge 회귀 예측
    # -----------------------------
    stable_df = base[base["status"] == "stable"].copy()
    stable_preds: dict[str, float] = {}

    if not stable_df.empty:
        X_train: list[list[float]] = []
        y_train: list[float] = []

        # 토큰별 6개 전이 샘플을 모아서 하나의 모델로 학습
        for token, r in stable_df.iterrows():
            Xr, yr = _build_training_rows(r)
            X_train.extend(Xr)
            y_train.extend(yr)

        # 학습 샘플이 너무 적으면(불안정) 안전하게 fallback
        if len(X_train) >= 50:
            model = Ridge(alpha=1.0, random_state=42)
            model.fit(np.array(X_train, dtype=float), np.array(y_train, dtype=float))

            X_infer = np.array([_build_infer_features(r) for _, r in stable_df.iterrows()], dtype=float)
            yhat_log = model.predict(X_infer)
            yhat = np.expm1(yhat_log)          # log1p 역변환
            yhat = np.clip(yhat, 0.0, None)    # 음수 방지

            for (token, _), v in zip(stable_df.iterrows(), yhat):
                stable_preds[str(token)] = float(v)
        else:
            # 학습 데이터가 부족할 때는 최근 3일 평균으로 대체(보수적)
            for token, r in stable_df.iterrows():
                stable_preds[str(token)] = float(r["avg_3d_count"])

    # -----------------------------
    # 트랙 2) 급상승형(burst) -> 룰 기반 예측
    # -----------------------------
    def _yhat_burst(r: pd.Series) -> float:
        c0, c1 = float(r["c0"]), float(r["c1"])
        doc0 = float(r["doc0"])
        delta = c0 - c1

        # 기본 예측: 오늘 + 상승분의 절반(보수적)
        y_base = c0 + 0.5 * max(delta, 0.0)

        # 폭주 방지 캡: ŷ <= (cap_multiplier * 오늘)
        y = min(y_base, params.yhat_cap_multiplier * c0)

        # (원칙상 후보 토큰은 doc0>=min_doc_today지만) 안전망으로 남겨둠
        if doc0 < params.min_doc_today:
            y *= 0.5

        return float(max(y, 0.0))

    # -----------------------------
    # 트랙 3) 불확실(uncertain) -> 보수적 예측
    # -----------------------------
    def _yhat_uncertain(r: pd.Series) -> float:
        avg_3d = float(r["avg_3d_count"])
        # 평균이 너무 작으면 그냥 오늘값으로 두는 편이 자연스러움(과도한 0 예측 방지)
        if avg_3d < 1.0:
            return float(r["c0"])
        return avg_3d

    # 상태별로 predicted_count 채우기
    base["predicted_count"] = 0.0
    for token, r in base.iterrows():
        status = r["status"]
        if status == "stable":
            base.at[token, "predicted_count"] = stable_preds.get(str(token), float(r["avg_3d_count"]))
        elif status == "burst":
            base.at[token, "predicted_count"] = _yhat_burst(r)
        else:
            base.at[token, "predicted_count"] = _yhat_uncertain(r)

    # -----------------------------
    # 최종 점수(score) 계산: 예측 카운트 + 버스트 부스팅
    # score = ŷ * (1 + w * sigmoid(burst))
    # -----------------------------
    def _w(status: str) -> float:
        if status == "burst":
            return params.w_burst
        if status == "stable":
            return params.w_stable
        return params.w_uncertain

    base["w"] = base["status"].apply(_w)
    base["score"] = base.apply(
        lambda r: float(r["predicted_count"]) * (1.0 + float(r["w"]) * _sigmoid(float(r["burst"]))),
        axis=1
    )

    # 인덱스(token)를 컬럼으로 되돌림
    out = base.reset_index().rename(columns={"index": "token"})

    # 점수 기준으로 정렬 후 Top-N 선택
    out = out.sort_values("score", ascending=False).head(params.top_n).copy()

    # 예측 날짜는 D+1
    predict_date = run_date + timedelta(days=1)
    out.insert(0, "predict_date", predict_date)

    # 저장용 컬럼만 골라서 정리 + 이름 표준화(today/yest/doc_today/doc_yest)
    out = out[
        [
            "predict_date",
            "token",
            "predicted_count",
            "score",
            "status",
            "burst",
            "c0",
            "c1",
            "avg_3d_count",
            "avg_7d_count",
            "doc0",
            "doc1",
        ]
    ].rename(
        columns={
            "c0": "today_count",
            "c1": "yest_count",
            "doc0": "doc_today",
            "doc1": "doc_yest",
        }
    )

    return out
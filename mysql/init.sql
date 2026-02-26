-- blog_db 초기화 스크립트
-- MySQL 컨테이너 최초 기동 시 /docker-entrypoint-initdb.d/ 에서 자동 실행된다.

CREATE DATABASE IF NOT EXISTS blog_db
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE blog_db;


-- =====================================================================
-- 1) Raw (원본) : blog_post
-- =====================================================================
-- blog.raw 토픽 → MySQL 저장 대상 테이블
-- url 을 PRIMARY KEY 로 설정하여 INSERT IGNORE 기반 중복 방지
CREATE TABLE IF NOT EXISTS blog_post (
    url           VARCHAR(2048) NOT NULL,
    title         TEXT          NOT NULL,
    content_text  LONGTEXT      NOT NULL,
    postdate      DATE,
    discovered_at DATETIME,
    PRIMARY KEY (url(512)),   -- MySQL은 LONGTEXT/TEXT의 인덱스 prefix 길이 제한이 있어 512자 지정
    KEY ix_postdate (postdate),
    KEY ix_discovered (discovered_at)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

-- =====================================================================
-- 2) Cleaned Doc (문서 단위 정제 데이터)
-- =====================================================================
CREATE TABLE IF NOT EXISTS cleaned_docs (
  doc_id CHAR(40) NOT NULL,                 -- 문서 고유 ID (url 기반 sha1 해시)
  url VARCHAR(2048) NOT NULL,               -- 원본 게시글 URL (중복 식별 키)
  doc_date DATE NOT NULL,                   -- 이 문서를 어느 날짜(D)로 묶을지 결정된 기준 날짜
  date_source VARCHAR(20) NOT NULL,         -- doc_date가 어떤 기준으로 결정됐는지 (postdate / discovered_at)
  postdate DATE NULL,                       -- 원본 데이터에 있는 게시글 작성일(있으면 우선 사용)
  discovered_at DATETIME NULL,              -- 크롤링/수집 시각 (postdate 없을 때 doc_date 산정에 사용)
  title TEXT NULL,                          -- 원본 제목
  content_text LONGTEXT NULL,               -- 원본 본문(정제 전)
  cleaned_text LONGTEXT NOT NULL,           -- 분석용 정제 텍스트(제목+본문 결합 후 URL/특수문자/공백 정리)
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 레코드 생성 시각(적재 시각)
  PRIMARY KEY (doc_id),                     -- 기본키: 문서 식별자
  UNIQUE KEY uq_url (url(512)),             -- URL 기준 중복 방지(인덱스 prefix 512)
  KEY ix_doc_date (doc_date)                -- 날짜 기준 조회/집계 성능용 인덱스
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================================
-- 3) Daily Token Stats (일별 키워드 집계)
-- =====================================================================
CREATE TABLE IF NOT EXISTS daily_token_stats (
  stat_date DATE NOT NULL,                  -- 집계 기준 날짜(D)
  token VARCHAR(100) NOT NULL,              -- 키워드/토큰(형태소 분석 또는 split 결과)
  mention_count INT NOT NULL,               -- 해당 날짜에 토큰이 등장한 총 횟수(문서 내 반복 포함)
  doc_count INT NOT NULL,                   -- 해당 날짜에 토큰이 등장한 문서 수(문서 내 중복 제거)
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 레코드 생성 시각(집계 적재 시각)
  PRIMARY KEY (stat_date, token),           -- 날짜×토큰 조합 유일(중복 집계 방지)
  KEY ix_token (token)                      -- 토큰 기준 조회(트렌드 추적) 성능용 인덱스
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================================
-- 4) Predictions (예측 결과)
-- =====================================================================
CREATE TABLE IF NOT EXISTS predictions (
  predict_date DATE NOT NULL,               -- 예측 대상 날짜(D+1)
  token VARCHAR(100) NOT NULL,              -- 예측한 키워드/토큰
  predicted_count DOUBLE NOT NULL,          -- 내일 예상 언급량(예측 카운트 ŷ)
  score DOUBLE NOT NULL,                    -- 랭킹 점수(예측 카운트 + 버스트 부스팅 반영)
  status VARCHAR(20) NOT NULL,              -- 토큰 상태 분류(stable/burst/uncertain)
  burst DOUBLE NOT NULL,                    -- 버스트 점수(최근 평균 대비 + 성장률 혼합 점수)
  today_count INT NOT NULL,                 -- 오늘(D) 실제 언급량(mention_count)
  yest_count INT NOT NULL,                  -- 어제(D-1) 실제 언급량(mention_count)
  avg_3d_count DOUBLE NOT NULL,             -- 최근 3일 평균 언급량(설명/디버깅용)
  avg_7d_count DOUBLE NOT NULL,             -- 최근 7일 평균 언급량(설명/디버깅용)
  doc_today INT NOT NULL,                   -- 오늘(D) 등장 문서 수(doc_count)
  doc_yest INT NOT NULL,                    -- 어제(D-1) 등장 문서 수(doc_count)
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- 예측 결과 저장 시각
  PRIMARY KEY (predict_date, token),        -- 예측일×토큰 유일(중복 예측 저장 방지)
  KEY ix_score (score)                      -- 점수 기준 정렬/Top-N 조회 성능용 인덱스
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================================
-- 5) Daily Reports (일일 보고서)
-- =====================================================================
CREATE TABLE IF NOT EXISTS daily_reports (
  report_date DATE NOT NULL,                 -- 보고서 기준 날짜
  title TEXT NOT NULL,                       -- 보고서 제목
  summary TEXT NULL,                         -- 요약
  content_md LONGTEXT NULL,                  -- 본문 (Markdown)
  keywords_json TEXT NULL,                   -- 키워드 JSON 배열 문자열
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

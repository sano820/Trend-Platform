-- blog_db 초기화 스크립트
-- MySQL 컨테이너 최초 기동 시 /docker-entrypoint-initdb.d/ 에서 자동 실행된다.

CREATE DATABASE IF NOT EXISTS blog_db
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE blog_db;

-- blog.raw 토픽 → MySQL 저장 대상 테이블
-- url 을 PRIMARY KEY 로 설정하여 INSERT IGNORE 기반 중복 방지
CREATE TABLE IF NOT EXISTS blog_post (
    url           VARCHAR(2048) NOT NULL,
    title         TEXT          NOT NULL,
    content_text  LONGTEXT      NOT NULL,
    postdate      DATE,
    discovered_at DATETIME,
    PRIMARY KEY (url(512))   -- MySQL은 LONGTEXT/TEXT의 인덱스 prefix 길이 제한이 있어 512자 지정
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;

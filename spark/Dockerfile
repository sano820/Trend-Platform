FROM bitnami/spark:3.5.1

USER root

# 파이썬 의존성 설치
COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# MySQL JDBC 드라이버 추가
# (Maven 없이도 Spark가 JDBC로 MySQL을 읽을 수 있게 jars에 넣어둠)
ARG MYSQL_JDBC_VERSION=8.4.0
RUN curl -L -o /opt/bitnami/spark/jars/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_JDBC_VERSION}/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar

# Spark 잡 코드 및 리소스 복사 (spark-batch 컨테이너에서 사용)
WORKDIR /opt/spark-app
COPY spark /opt/spark-app/spark

# bitnami 이미지 기본값인 non-root 사용자(1001)로 되돌리기
USER 1001

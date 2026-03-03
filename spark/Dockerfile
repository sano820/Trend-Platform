FROM spark:4.0.1-python3

USER root

# apache/spark 이미지에는 pip이 없을 수 있어 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3-pip curl \
    && rm -rf /var/lib/apt/lists/*

# 파이썬 의존성 설치
COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# MySQL JDBC 드라이버 추가
# (Maven 없이도 Spark가 JDBC로 MySQL을 읽을 수 있게 jars에 넣어둠)
ARG MYSQL_JDBC_VERSION=8.4.0
RUN curl -L -o /opt/spark/jars/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_JDBC_VERSION}/mysql-connector-j-${MYSQL_JDBC_VERSION}.jar

# Spark 잡 코드 및 리소스 복사 (spark-batch 컨테이너에서 사용)
WORKDIR /opt/spark-app
COPY spark /opt/spark-app/spark

# apache/spark 기본 유저로 되돌리기 (없으면 root 유지)
# USER 185

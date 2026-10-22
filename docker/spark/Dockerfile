# =============================================================================
#  Image Spark (master + worker) — Spark 3.5.4 via PySpark, Python 3.11, Java 17.
#  Pourquoi pas apache/spark:3.5.x ? Elle embarque Python 3.8 alors que le
#  driver (Airflow) tourne en Python 3.11 : PySpark refuse les versions
#  mineures différentes entre driver et executors. On aligne donc tout sur 3.11.
#  Contexte de build : ./docker
# =============================================================================
FROM python:3.11-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

COPY spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

COPY common/download_jars.sh /tmp/download_jars.sh
# sed : neutralise d'éventuels CRLF Windows dans le script
RUN sed -i 's/\r$//' /tmp/download_jars.sh && sh /tmp/download_jars.sh "${SPARK_HOME}/jars"

WORKDIR /opt/spark-work
ENTRYPOINT ["/usr/bin/tini", "--"]

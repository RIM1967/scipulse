# =============================================================================
#  Image Airflow SciPulse — Airflow 2.10.5 + Java 17 + PySpark 3.5.4 + GX 1.x
#  Les tâches Airflow jouent le rôle de *driver* Spark (mode client) : il faut
#  donc Java et les mêmes JARs que les executors.
#  Contexte de build : ./docker
# =============================================================================
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

USER airflow
# Installation sous les contraintes officielles Airflow : évite qu'une
# dépendance transitive casse Airflow (bonne pratique documentée par Apache).
COPY airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=python3

USER root
COPY common/download_jars.sh /tmp/download_jars.sh
RUN sed -i 's/\r$//' /tmp/download_jars.sh && sh /tmp/download_jars.sh "${SPARK_HOME}/jars"
USER airflow

#!/bin/sh
# =============================================================================
#  Télécharge les JARs Spark nécessaires au pipeline dans le dossier $1.
#  Partagé par les images Spark (executors) et Airflow (driver) : les deux
#  doivent avoir exactement les mêmes connecteurs sur leur classpath.
#
#  Pourquoi pré-installer plutôt que `spark.jars.packages` ?
#    - pas de résolution Ivy au runtime (plus rapide, fonctionne hors-ligne) ;
#    - versions figées dans l'image = exécution reproductible.
# =============================================================================
set -eu

DEST="${1:?usage: download_jars.sh <dossier_jars>}"
MAVEN="https://repo1.maven.org/maven2"

for url in \
    "${MAVEN}/org/elasticsearch/elasticsearch-spark-30_2.12/8.13.4/elasticsearch-spark-30_2.12-8.13.4.jar" \
    "${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
    "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar" \
    "https://repos.spark-packages.org/graphframes/graphframes/0.8.3-spark3.5-s_2.12/graphframes-0.8.3-spark3.5-s_2.12.jar"
do
    echo "-> $(basename "$url")"
    # --retry-all-errors : réessaie aussi les transferts coupés en route
    # (code 18), fréquents sur le JAR AWS de ~280 Mo
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 3 \
        -o "${DEST}/$(basename "$url")" "$url"
done

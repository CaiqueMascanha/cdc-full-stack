#!/bin/bash
SCRIPT="$1"
shift

# Se não começar com /, adiciona o prefixo
if [[ "$SCRIPT" != /* ]]; then
  SCRIPT="/opt/spark-jobs/$SCRIPT"
fi

/opt/spark/bin/spark-submit \
  --jars /opt/spark-jobs/jars/spark-sql-kafka-4.0-scala213.jar,/opt/spark-jobs/jars/kafka-clients.jar,/opt/spark-jobs/jars/spark-token-provider-4.0-scala213.jar,/opt/spark-jobs/jars/commons-pool2.jar,/opt/spark-jobs/jars/delta-spark.jar,/opt/spark-jobs/jars/delta-storage.jar,/opt/spark-jobs/jars/hadoop-aws.jar,/opt/spark-jobs/jars/aws-sdk-bundle.jar \
  "$SCRIPT" "$@"
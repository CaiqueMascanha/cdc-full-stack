#!/bin/bash
/opt/spark/bin/spark-submit \
  --jars /opt/spark-jobs/jars/spark-sql-kafka-4.0-scala213.jar,/opt/spark-jobs/jars/kafka-clients.jar,/opt/spark-jobs/jars/spark-token-provider-4.0-scala213.jar,/opt/spark-jobs/jars/commons-pool2.jar \
  "$@"
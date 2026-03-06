import sys
sys.path.insert(0, '/opt/spark-jobs')

import pyspark.sql.functions as F
from utils.spark_session import create_session
from jobs.emprestimos.schemas import schema_debezium

KAFKA_BROKER   = "cdc-kafka:9092"
KAFKA_TOPIC    = "cdc.public.emprestimos"
CHECKPOINT_DIR = "/tmp/checkpoints/emprestimos"

spark = create_session("CDC - Emprestimos", CHECKPOINT_DIR)

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("minPartitions", "1") \
    .load()

df_parsed = df_raw \
    .selectExpr("CAST(value as STRING) as json_str") \
    .select(F.from_json(F.col('json_str'), schema_debezium).alias('d')) \
    .select(
        F.col('d.payload.op').alias('operacao'),
        F.col('d.payload.ts_ms').alias('ts_ms'),
        F.col('d.payload.after.*')
    ) \
    .filter(F.col('operacao').isin(['c', 'u'])) \
    .withColumn('event_time', F.to_timestamp(F.col('ts_ms') / 1000)) \
    .withColumn('data', F.to_date(F.col('event_time')))

query = df_parsed \
    .filter(F.col('operacao') == 'c') \
    .select("event_time", "id", "cliente_nome",
            "valor_solicitado", "parcelas_solicitadas",
            "finalidade", "status") \
    .writeStream \
    .queryName('emprestimos_inserts') \
    .outputMode('append') \
    .format('console') \
    .option('truncate', False) \
    .option('numRows', 10) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()
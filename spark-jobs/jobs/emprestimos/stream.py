import sys
sys.path.insert(0, '/opt/spark-jobs')

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from utils.spark_session import create_session
from jobs.emprestimos.schemas import schema_debezium_solicitados, schema_debezium_aprovados

KAFKA_BROKER   = "cdc-kafka:9092"
KAFKA_TOPIC_SOLICITADOS_INPUT  = "cdc.public.emprestimos"
KAFKA_TOPIC_APROVADOS_INPUT    = "cdc.public.emprestimos_aprovados"
KAFKA_TOPIC_SOLICITADOS_OUTPUT = "cdc.public.emprestimos_solicitados_agregado_diario"
KAFKA_TOPIC_APROVADOS_OUTPUT   = "cdc.public.emprestimos_aprovados_agregado_diario"
CHECKPOINT_DIR = "/tmp/checkpoints/emprestimos"
CHECKPOINT_AGREGADO_DIR = "/tmp/checkpoints/emprestimos_solicitados_agregados"
CHECKPOINT_APROVADOS_DIR = "/tmp/checkpoints/emprestimos_aprovados"

spark = create_session("CDC - Emprestimos", CHECKPOINT_DIR)
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

window_latest = Window.partitionBy('id').orderBy(F.col('ts_ms').desc())

df_raw_solicitados = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC_SOLICITADOS_INPUT) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("minPartitions", "1") \
    .load()
    
df_raw_aprovados = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC_APROVADOS_INPUT) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("minPartitions", "1") \
    .load()

df_parsed_solicitados = df_raw_solicitados \
    .selectExpr("CAST(value as STRING) as json_str") \
    .select(F.from_json(F.col('json_str'), schema_debezium_solicitados).alias('d')) \
    .select(
        F.col('d.payload.op').alias('operacao'),
        F.col('d.payload.ts_ms').alias('ts_ms'),
        F.col('d.payload.after.*')
    ) \
    .filter(F.col('operacao').isin(['c', 'u'])) \
    .withColumn('event_time', F.to_timestamp(F.col('ts_ms') / 1000)) \
    .withColumn('data', F.to_date(F.col('event_time')))
    
df_parsed_aprovados = df_raw_aprovados \
    .selectExpr("CAST(value as STRING) as json_str") \
    .select(F.from_json(F.col('json_str'), schema_debezium_aprovados).alias('d')) \
    .select(
        F.col('d.payload.op').alias('operacao'),
        F.col('d.payload.ts_ms').alias('ts_ms'),
        F.col('d.payload.after.*')
    ) \
    .filter(F.col('operacao').isin(['c', 'u'])) \
    .withColumn('event_time', F.to_timestamp(F.col('ts_ms') / 1000)) \
    .withColumn('data', F.to_date(F.col('event_time')))

# query = df_parsed__solicitados \
#     .filter(F.col('operacao') == 'c') \
#     .select("event_time", "id", "cliente_nome",
#             "valor_solicitado", "parcelas_solicitadas",
#             "finalidade", "status") \
#     .writeStream \
#     .queryName('emprestimos_inserts') \
#     .outputMode('append') \
#     .format('console') \
#     .option('truncate', False) \
#     .option('numRows', 10) \
#     .trigger(processingTime='5 seconds') \
#     .start()
    
df_agregado_solicitados = df_parsed_solicitados \
    .filter(F.col('operacao').isin(['c'])) \
    .withColumn('data_solicitado', F.to_date(F.to_timestamp(F.col('solicitado_em').cast('long') / 1_000))) \
    .groupBy('data_solicitado') \
    .agg(
        F.sum(F.when(F.col('operacao') == 'c', 1).otherwise(0)).alias('total_emprestimos_solicitados'),
        F.sum(F.when(F.col('operacao') == 'c', F.col('valor_solicitado')).otherwise(0)).alias('total_valor_solicitado')
    ) \
    .select(
        F.col('data_solicitado').cast('string').alias('key'),
        F.to_json(F.struct(
            F.col('data_solicitado').cast('string').alias('data_solicitado'),
            F.col('total_emprestimos_solicitados'),
            F.col('total_valor_solicitado')
        )).alias('value')
    ) \
    .filter(F.col('data_solicitado') == F.current_date())
    
df_agregado_aprovados = df_parsed_aprovados \
    .filter(F.col('operacao').isin(['c'])) \
    .withColumn('data_aprovado', F.to_date(F.to_timestamp(F.col('aprovado_em').cast('long') / 1_000))) \
    .groupBy('data_aprovado') \
    .agg(
        F.sum(F.when(F.col('operacao') == 'c', 1).otherwise(0)).alias('total_emprestimos_aprovados'),
        F.sum(F.when(F.col('operacao') == 'c', F.col('valor_aprovado')).otherwise(0)).alias('total_valor_aprovado')
    ) \
    .select(
        F.col('data_aprovado').cast('string').alias('key'),
        F.to_json(F.struct(
            F.col('data_aprovado').cast('string').alias('data_aprovado'),
            F.col('total_emprestimos_aprovados'),
            F.col('total_valor_aprovado')
        )).alias('value')
    ) \
    .filter(F.col('data_aprovado') == F.current_date())
    
query_kafka_solicitados = df_agregado_solicitados \
    .writeStream \
    .queryName('emprestimos_agregado_solicitados') \
    .outputMode('complete') \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BROKER) \
    .option('topic', KAFKA_TOPIC_SOLICITADOS_OUTPUT) \
    .option('checkpointLocation', CHECKPOINT_AGREGADO_DIR) \
    .trigger(processingTime='10 seconds') \
    .start()
    
query_kafka_aprovados = df_agregado_aprovados \
    .writeStream \
    .queryName('emprestimos_agregado_aprovados') \
    .outputMode('complete') \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BROKER) \
    .option('topic', KAFKA_TOPIC_APROVADOS_OUTPUT) \
    .option('checkpointLocation', CHECKPOINT_APROVADOS_DIR) \
    .trigger(processingTime='10 seconds') \
    .start()
    
spark.streams.awaitAnyTermination()
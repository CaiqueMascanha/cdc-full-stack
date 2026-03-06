from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import json

KAFKA_BROKER   = "cdc-kafka:9092"
KAFKA_TOPIC    = "cdc.public.emprestimos"
CHECKPOINT_DIR = "/tmp/checkpoints/emprestimos"

spark = SparkSession.builder \
    .appName('CDC - Emprestimos') \
    .master('spark://spark-master:7077') \
    .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR) \
    .getOrCreate()
    
spark.sparkContext.setLogLevel('WARN')

schema_emprestimo = T.StructType([
    T.StructField("id",                   T.LongType()),
    T.StructField("cliente_nome",         T.StringType()),
    T.StructField("cliente_cpf",          T.StringType()),
    T.StructField("cliente_email",        T.StringType()),
    T.StructField("valor_solicitado",     T.DoubleType()),
    T.StructField("taxa_juros_proposta",  T.DoubleType()),
    T.StructField("parcelas_solicitadas", T.IntegerType()),
    T.StructField("finalidade",           T.StringType()),
    T.StructField("status",               T.StringType()),
    T.StructField("motivo_reprovacao",    T.StringType()),
    T.StructField("solicitado_em",        T.StringType()),
    T.StructField("atualizado_em",        T.StringType()),
])

schema_debezium = T.StructType([
    T.StructField("payload", T.StructType([
        T.StructField("before", schema_emprestimo),
        T.StructField("after",  schema_emprestimo),
        T.StructField("op",     T.StringType()),
        T.StructField("ts_ms",  T.LongType()),
    ]))
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("minPartitions", "1") \
    .option("kafka.group.id", "spark-cdc-consumer") \
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
    
query_raw = df_parsed \
    .filter(F.col('operacao') == 'c') \
    .select(
        "event_time", "id", "cliente_nome",
        "valor_solicitado", "parcelas_solicitadas",
        "finalidade", "status"
    ) \
    .writeStream \
    .queryName('raw_inserts')  \
    .outputMode('append')  \
    .format('console')  \
    .option('truncate', False)  \
    .option('numRows', 10) \
    .trigger(processingTime='5 seconds') \
    .start()
    
query_raw.awaitTermination()
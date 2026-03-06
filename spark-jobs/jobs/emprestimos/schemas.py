import pyspark.sql.types as T

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
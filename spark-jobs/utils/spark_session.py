from pyspark.sql import SparkSession

def create_session(app_name: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .master('spark://spark-master:7077') \
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .config('spark.hadoop.fs.s3a.endpoint', 'http://cdc-minio:9000') \
        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin123') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.connection.timeout', '200000') \
        .config('spark.hadoop.fs.s3a.socket.timeout', '200000') \
        .config('spark.hadoop.fs.s3a.connection.establish.timeout', '5000') \
        .config('spark.hadoop.fs.s3a.attempts.maximum', '3') \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel('WARN')
    return spark
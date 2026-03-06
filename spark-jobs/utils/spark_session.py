from pyspark.sql import SparkSession

def create_session(app_name: str, checkpoint_dir: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .master('spark://spark-master:7077') \
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .config('spark.hadoop.fs.s3a.endpoint', 'http://cdc-minio:9000') \
        .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel('WARN')
    return spark
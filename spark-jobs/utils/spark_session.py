from pyspark.sql import SparkSession

def create_session(app_name: str, checkpoint_dir: str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(app_name) \
        .master('spark://spark-master:7077') \
        .config('spark.sql.streaming.checkpointLocation', checkpoint_dir) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark
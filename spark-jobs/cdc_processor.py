from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
          .appName('CDC Processor') \
          .master('spark://spark-master:7077') \
          .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# DataFrame de teste
data = [
    (1, "Pedro", "pendente", 150.0),
    (2, "Ana",   "aprovado", 320.5),
    (3, "João",  "cancelado", 89.9),
]
columns = ["id", "cliente", "status", "valor"]

df = spark.createDataFrame(data, columns)

df.show()
df.printSchema()

print(f"\nTotal de registros: {df.count()}")

spark.stop()
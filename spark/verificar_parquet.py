
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerificarParquetUrbanMove") \
    .getOrCreate()

ruta = "/app/datos/parquet_unificado"

df = spark.read.parquet(ruta)

print("\n========== VERIFICACION URBANMOVE ==========")
print("TOTAL DE REGISTROS:", df.count())
print("\nCOLUMNAS:")
df.printSchema()

spark.stop()

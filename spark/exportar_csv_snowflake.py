
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExportarUrbanMoveSnowflake") \
    .getOrCreate()

# Leer el Parquet que ya verificamos
df = spark.read.parquet("/app/datos/parquet_unificado")

# Comprobar cantidad
print("Registros encontrados:", df.count())

# Exportar todos los registros a CSV
df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("encoding", "UTF-8") \
    .csv("/app/datos/csv_snowflake")

print("Exportación CSV terminada.")

spark.stop()

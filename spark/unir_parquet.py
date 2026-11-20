
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UrbanMove-Unir-Parquet") \
    .getOrCreate()

print("Leyendo archivos Parquet...")

datos = spark.read.parquet("/app/datos/parquet/movilidad")

print("Registros encontrados:", datos.count())

print("Generando archivo unificado...")

datos.coalesce(1).write \
    .mode("overwrite") \
    .parquet("/app/datos/parquet_unificado")

print("Archivo Parquet unificado correctamente.")

spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("UrbanMove-Exportar-Parquet") \
    .getOrCreate()

print("Iniciando exportación de datos...")

# Leer archivo CSV
datos = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/app/datos_movilidad.csv")

# Ajustar tipos de datos
datos = datos \
    .withColumn("id", col("id").cast("integer")) \
    .withColumn("latitud", col("latitud").cast("double")) \
    .withColumn("longitud", col("longitud").cast("double")) \
    .withColumn("velocidad", col("velocidad").cast("double")) \
    .withColumn("fecha_hora", to_timestamp(col("fecha_hora")))

# Crear columna de fecha para particionar
datos = datos.withColumn(
    "fecha_dia",
    to_date(col("fecha_hora"))
)

# Mostrar cantidad de registros
total = datos.count()

print("Total de registros:", total)

# Guardar en formato Parquet
datos.write \
    .mode("overwrite") \
    .partitionBy("fecha_dia") \
    .parquet("/app/datos/parquet/movilidad")

print("Exportacion a Parquet completada.")

spark.stop()

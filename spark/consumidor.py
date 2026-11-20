
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, avg, count, when, round, to_date
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType
)

# ==========================================
# 1. CONFIGURACION DE SPARK
# ==========================================

spark = SparkSession.builder \
    .appName("UrbanMove-Streaming") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("====================================")
print(" URBANMOVE - SPARK STRUCTURED STREAMING")
print("====================================")


# ==========================================
# 2. ESQUEMA DE LOS DATOS GPS
# ==========================================

esquema = StructType([
    StructField("id", IntegerType(), True),
    StructField("vehiculo_id", StringType(), True),
    StructField("ruta_id", StringType(), True),
    StructField("latitud", DoubleType(), True),
    StructField("longitud", DoubleType(), True),
    StructField("velocidad", DoubleType(), True),
    StructField("fecha_hora", TimestampType(), True),
    StructField("estacion_id", StringType(), True)
])


# ==========================================
# 3. LECTURA DE KAFKA
# ==========================================

datos_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "movilidad-gps") \
    .option("startingOffsets", "earliest") \
    .load()

datos_json = datos_kafka.select(
    from_json(
        col("value").cast("string"),
        esquema
    ).alias("datos")
)

datos = datos_json.select("datos.*")

datos = datos.filter(
    col("fecha_hora").isNotNull() &
    col("velocidad").isNotNull() &
    col("ruta_id").isNotNull() &
    col("vehiculo_id").isNotNull()
)


# ==========================================
# 4. GUARDAR LECTURAS GPS EN CASSANDRA
# ==========================================

def guardar_en_cassandra(lote, numero_lote):

    if lote.isEmpty():
        return

    datos_cassandra = lote.withColumn(
        "fecha_dia",
        to_date(col("fecha_hora"))
    )

    datos_cassandra = datos_cassandra.select(
        "vehiculo_id",
        "fecha_dia",
        "fecha_hora",
        "id",
        "ruta_id",
        "latitud",
        "longitud",
        "velocidad",
        "estacion_id"
    )

    datos_cassandra.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(
            table="lecturas_gps",
            keyspace="urbanmove"
        ) \
        .save()

    print(f"Lote {numero_lote} guardado en Cassandra.")


consulta_cassandra = datos.writeStream \
    .foreachBatch(guardar_en_cassandra) \
    .option(
        "checkpointLocation",
        "/app/spark/checkpoints/cassandra_gps"
    ) \
    .start()


# ==========================================
# 5. PROCESAMIENTO DE CONGESTION
# ==========================================

resultados = datos \
    .withWatermark("fecha_hora", "2 minutes") \
    .groupBy(
        window(col("fecha_hora"), "1 minute"),
        col("ruta_id")
    ) \
    .agg(
        round(avg("velocidad"), 2).alias("velocidad_promedio"),
        count("*").alias("total_eventos")
    )

resultados = resultados.withColumn(
    "estado",
    when(
        col("velocidad_promedio") < 10,
        "POSIBLE CONGESTION"
    ).otherwise("FLUIDO")
)

salida = resultados.select(
    col("window.start").alias("inicio_ventana"),
    col("window.end").alias("fin_ventana"),
    col("ruta_id"),
    col("velocidad_promedio"),
    col("total_eventos"),
    col("estado")
)


# ==========================================
# 6. MOSTRAR RESULTADOS EN CONSOLA
# ==========================================

consulta = salida.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", "30") \
    .option(
        "checkpointLocation",
        "/app/spark/checkpoints/urbanmove"
    ) \
    .trigger(processingTime="10 seconds") \
    .start()


print("Procesamiento de eventos GPS iniciado.")
print("Guardando lecturas en Cassandra...")
print("Analizando posibles congestiones...")

spark.streams.awaitAnyTermination()

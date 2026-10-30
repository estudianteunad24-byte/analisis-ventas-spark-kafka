from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    count,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType
)

# 1. Crear la sesion de Spark
spark = (
    SparkSession.builder
    .appName("AnalisisVentasKafka")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Definir la estructura del mensaje
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("producto", StringType(), True),
    StructField("cantidad", IntegerType(), True),
    StructField("precio", DoubleType(), True),
    StructField("ingresos", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 3. Leer mensajes desde Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ventas")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Convertir el JSON en columnas
ventas = (
    df.select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("dato")
    )
    .select("dato.*")
    .filter(col("id").isNotNull())
    .filter(col("ingresos").isNotNull())
)

# 5. Agrupar las ventas por ventanas de 10 segundos
resumen = (
    ventas
    .withWatermark("timestamp", "20 seconds")
    .groupBy(
        window(col("timestamp"), "10 seconds")
    )
    .agg(
        count("*").alias("numero_ventas"),
        spark_sum("ingresos").alias("ingresos_totales")
    )
)

# 6. Mostrar resultados en consola
consulta = (
    resumen.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 20)
    .trigger(processingTime="5 seconds")
    .start()
)

consulta.awaitTermination()

# 1. Crear sesion de Spark
spark = (
    SparkSession.builder
    .appName("AnalisisVentasBatch")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar los datos historicos
ventas = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("datos/ventas.csv")
)

# 3. Mostrar los datos iniciales
print("DATOS HISTORICOS DE VENTAS")
ventas.show(10)

# 4. Eliminar registros incompletos
ventas = ventas.dropna(
    subset=["producto", "cantidad", "precio"]
)

# 5. Calcular ingresos si no existen
if "ingresos" not in ventas.columns:
    ventas = ventas.withColumn(
        "ingresos",
        col("cantidad") * col("precio")
    )

# 6. Calcular indicadores generales
print("RESUMEN GENERAL DE VENTAS")

ventas.agg(
    count("*").alias("numero_ventas"),
    spark_sum("cantidad").alias("unidades_vendidas"),
    spark_sum("ingresos").alias("ingresos_totales"),
    avg("ingresos").alias("promedio_por_venta")
).show()

# 7. Analizar ventas por producto
print("VENTAS POR PRODUCTO")

resumen_productos = (
    ventas.groupBy("producto")
    .agg(
        count("*").alias("numero_ventas"),
        spark_sum("cantidad").alias("unidades_vendidas"),
        spark_sum("ingresos").alias("ingresos_totales")
    )
    .orderBy(desc("ingresos_totales"))
)

resumen_productos.show()

# 8. Guardar resultados
resumen_productos.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("resultados/ventas_por_producto")

print("ANALISIS BATCH FINALIZADO")

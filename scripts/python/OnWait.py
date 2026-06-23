from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("ReceptorMultiTabla").getOrCreate()

# ==========================================
# FLUJO 1: CLIENTES (Puerto 9998)
# ==========================================
esquema_clientes = StructType() \
    .add("id_cliente", StringType()) \
    .add("edad", IntegerType()) \
    .add("ingreso", DoubleType()) # Agrega tus columnas reales

flujo_clientes = spark.readStream.format("socket").option("host", "0.0.0.0").option("port", 9998).load()

datos_clientes = flujo_clientes.select(from_json(col("value"), esquema_clientes).alias("datos")).select("datos.*")

query_clientes = datos_clientes.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/app/datalake/_checkpoints/clientes") \
    .start("/app/datalake/Clientes")


# ==========================================
# FLUJO 2: HISTORIAL (Puerto 9999)
# ==========================================
esquema_historial = StructType() \
    .add("id_cliente", StringType()) \
    .add("id_producto", StringType()) \
    .add("saldo", DoubleType()) # Agrega tus columnas reales

flujo_historial = spark.readStream.format("socket").option("host", "0.0.0.0").option("port", 9999).load()

datos_historial = flujo_historial.select(from_json(col("value"), esquema_historial).alias("datos")).select("datos.*")

query_historial = datos_historial.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/app/datalake/_checkpoints/historial") \
    .start("/app/datalake/Historial")


# ==========================================
# ENCENDER LOS MOTORES SIMULTÁNEOS
# ==========================================
# Esto mantiene vivos AMBOS flujos indefinidamente
spark.streams.awaitAnyTermination()
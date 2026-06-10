

from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import when, sum, col, explode, sequence, lit, rand, monotonically_increasing_id, round, broadcast
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler


#from h3 import h3 # Librería de Uber



SparkDelta = SparkSession.builder.appName("Export2Delta").master("local[*]").config("spark.driver.memory", "60g") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Rutas (Usa r"" para evitar problemas con las diagonales en Windows)
ClientDeltaPATH = r"C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/datalake/clientes_delta"
HistorDeltaPATH = r"C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/datalake/historial_delta"

# LEER COMO DELTA, NO COMO PARQUET
df_cliente_delta   = SparkDelta.read.format("delta").load(ClientDeltaPATH)
df_historial_delta = SparkDelta.read.format("delta").load(HistorDeltaPATH)

print(f"Total Clientes: {df_cliente_delta.count()}")
print(f"Total Historial: {df_historial_delta.count()}")



# Agregamos el historial para tener métricas por cliente
df_features_hist = df_historial_delta.groupBy("id_cliente").agg(
    F.avg("monto_transaccion").alias("avg_gasto"),
    F.stddev("monto_transaccion").alias("std_gasto"),
    F.count("id_transaccion").alias("frecuencia_uso"),
    F.max("monto_transaccion").alias("max_gasto")
)

# Unimos con los datos base del cliente
df_perfilado = df_cliente_delta.join(df_features_hist, on="id_cliente", how="inner")


from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# 1. Vectorizar las columnas numéricas
columnas_ml = ["edad", "ingreso_mensual", "avg_gasto", "std_gasto", "frecuencia_uso"]
assembler = VectorAssembler(inputCols=columnas_ml, outputCol="features_raw")

# 2. Escalar (Z-score)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)

# 3. Entrenar K-Means (Ejemplo con k=5 clústeres)
kmeans = KMeans(k=5, seed=42)
pipeline_model = kmeans.fit(scaler.fit(assembler.transform(df_perfilado)).transform(assembler.transform(df_perfilado)))

# Asignar clústeres
df_con_cluster = pipeline_model.transform(scaler.fit(assembler.transform(df_perfilado)).transform(assembler.transform(df_perfilado)))



# Creamos el índice por hexágono
df_h3_index = (
    df_con_cluster.groupBy("h3_index_res8")
    .agg( F.avg("prediction").alias("cluster_promedio"), F.avg("ingreso_mensual").alias("ingreso_zona"),
          F.count("id_cliente").alias("densidad_clientes")
          )
)

# Creamos un score normalizado (ejemplo simple)
df_h3_index = df_h3_index.withColumn( "indice_potencial", (F.col("ingreso_zona") * F.col("densidad_clientes")) / F.lit(1000) )

# Guardamos el dataset maestro ya con clústeres e índices H3
df_master_ml = df_con_cluster.join(df_h3_index, on="h3_index_res8", how="left")

df_master_ml.write.format("delta").mode("overwrite").save(r"C:/.../data/master_ml_ready")



from pyspark.sql import functions as F

# 1. Definimos las métricas base por cliente
df_metrics = df_cliente_delta.select( "id_cliente", "h3_index_res8", "ingreso_mensual", "gasto_mensual", "capacidad_ahorro" )

# 2. Obtenemos estadísticas globales (Contexto Nacional)
stats = df_metrics.select(
    F.avg("ingreso_mensual").alias("avg_ingreso"),
    F.stddev("ingreso_mensual").alias("std_ingreso"),
    F.avg("gasto_mensual").alias("avg_gasto"),
    F.stddev("gasto_mensual").alias("std_gasto")
).collect()[0]

# 3. Aplicamos el cálculo del Z-score individual
df_z_score = df_metrics.withColumn(
    "z_ingreso", (F.col("ingreso_mensual") - stats["avg_ingreso"]) / stats["std_ingreso"]
).withColumn(
    "z_gasto", (F.col("gasto_mensual") - stats["avg_gasto"]) / stats["std_gasto"]
)


# Agrupamos por hexágono para obtener el índice territorial
df_h3_stats = df_z_score.groupBy("h3_index_res8").agg(
    F.avg("z_ingreso").alias("indice_ingreso_h3"),
    F.avg("z_gasto").alias("indice_gasto_h3"),
    F.count("id_cliente").alias("volumen_clientes")
)

# Creamos un índice compuesto: Z-score combinado
# Esto ayuda a identificar zonas de "Alto Valor / Alto Gasto"
df_h3_stats = df_h3_stats.withColumn(
    "score_perfilado_v1",
    (F.col("indice_ingreso_h3") + F.col("indice_gasto_h3")) / 2
)














import os
import sys
import importlib
import pandas as pd
from functools import partial

# 1. CONFIGURACIÓN DE ENTORNO (Indispensable para Windows)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import Gene_hist
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Forzar recarga de lógica personalizada
importlib.reload(Gene_hist)
from Gene_hist import generar_historial_spark, generar_lote_clientes_Spark, calculate_distance_O, generar_historial_total_spark


# 2. SESIÓN ÚNICA DE SPARK (Delta 4.1.0 + Estabilidad Windows)
# He ajustado local[8] para que Windows tenga 4 hilos libres para los workers de Python
import os
import sys
from pyspark.sql import SparkSession



SparkLote = SparkSession.builder.appName("GeneracionLote").master("local[12]") \
    .config("spark.driver.memory", "70g") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

SparkLote.sparkContext.setCheckpointDir("data/checkpoints")

# 3. CARGA DE RECURSOS Y GENERACIÓN DE LOTE
PATH_DATALAKE = "C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/datalake/clientes_delta"
df_mun_pd = pd.read_csv("C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/poblacion.csv")


num_clientes = 3000000

# Salarios base por entidad (Corregidos)
data_salarios = [(i + 1, s) for i, s in enumerate([
    29939, 33729, 34909, 22078, 29217, 26233, 14352, 28651, 36895, 21566,
    22345, 15214, 19543, 28540, 23315, 20123, 21456, 21890, 39011, 15432,
    18765, 27432, 25678, 20987, 24321, 28912, 19876, 24567, 17654, 17234,
    23456, 19321
])]
df_salarios_base = pd.DataFrame(data_salarios, columns=["cve_ent", "salario_base"])

# Cálculo previo de probabilidades
total_pob = df_mun_pd['poblacion'].sum()
df_mun_pd['prob'] = df_mun_pd['poblacion'] / total_pob


def obtener_ultimo_id(ruta):
    try:
        # Leemos la carpeta directamente como parquet, saltando el motor de Delta
        return  SparkLote.read.parquet(ruta).select(F.max("id_cliente")).collect()[0][0] or 0
    except Exception:
        # Si la carpeta no existe o está vacía
        return 0

ultimo_id = obtener_ultimo_id(PATH_DATALAKE)
print(f"Último ID detectado: {ultimo_id}")
# 2. Generar el nuevo lote empezando desde el siguiente número
#ruta_temp = "data/temp.parquet"

df_nuevo_lote = generar_lote_clientes_Spark(num_clientes, df_mun_pd, df_salarios_base, id_inicial=ultimo_id + 1)
df_nuevo_lote = df_nuevo_lote.repartition(720, "id_cliente")
#df_nuevo_lote.show(100)
#df_nuevo_lote.write.mode("overwrite").parquet( "data/lote_clientes_temp.parquet")  #.partitionBy("estado")
#df_nuevo_lote =  SparkLote.read.parquet("data/temp.parquet")
#df_nuevo_lote = df_nuevo_lote.repartition(720, "id_cliente")
#df_nuevo_lote.cache()
#df_nuevo_lote =  SparkLote.read.parquet("data/lote_clientes_temp.parquet")
#lista_empleos = ["Jornalero", "Agricultor", "Empleado de bajo nivel", "Vendedor", "Atención al cliente", "Emprendedor",
#                 "Limpiador", "Transportista", "Jefa(e) de casa", "Dueño de negocio", "Uber", "Autoempleado",
#                 "Comerciante",
#                 "Albañil", "Otros trabajos", "Desempleo", "Desempleo"]
#EmpleosLvl0 = F.array([F.lit(x) for x in lista_empleos])
#
#lista_empleos = ["Agricultor", "Veterinario", "Ganadero", "Empleado de bajo nivel", "Atención al cliente",
#                 "Emprendedor",
#                 "Jefa(e) de casa", "Dueño de negocio", "Uber", "Autoempleado", "Comerciante", "Empleado de bajo nivel",
#                 "Empleado de nivel Medio", "Gerente", "Emprendedor", "Jefa(e) de casa", "Dueño de negocio", "Uber",
#                 "Maestro", "Tecnico", "Enfermera", "Medico General", "Cirujano", "Servidor Publico",
#                 "Profesor Universitario",
#                 "Tecnico", "Arquitecto", "Ingeniero", "Albañil", "Abogado", "Artista", "Psicologo", "Tecnico",
#                 "Empleado de bajo nivel", "Analista", "Programador", "Ingeniero industrial", "Otros trabajos",
#                 "Desempleo",
#                 "Desempleo", "Experto en redes sociales", "Desempleo", "Voluntario"]
#EmpleosLvl1 = F.array([F.lit(x) for x in lista_empleos])

#df_nuevo_lote = df_nuevo_lote.withColumns({  #  SparkLote.range(0, num_clientes)
#    "EMPLEO": F.when(F.col("nivel_edu").isin(["Sin Educación", "Primaria", "Secundaria", "Bachiller"]),
#                    F.element_at(EmpleosLvl0, (F.rand() * F.size(EmpleosLvl0) + 1).cast("int")))
#               .otherwise(
#                    F.element_at(EmpleosLvl1, (F.rand() * F.size(EmpleosLvl1) + 1).cast("int"))
#                    )})

EducacionBasica = ["Sin Educación", "Primaria", "Secundaria"]
df_nuevo_lote = df_nuevo_lote.withColumns({
    "NUM_DEP":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.rand() * 6).cast("int"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.rand() * 5).cast("int"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.rand() * 4).cast("int"))
        .otherwise((F.rand() * 2).cast("int")),
    "tiene_auto":
        F.when(F.col("nivel_edu").isin(EducacionBasica), F.when(F.rand() > 0.1, "Sí").otherwise("No"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), F.when(F.rand() > 0.25, "Sí").otherwise("No"))
        .when(F.col("nivel_edu").isin(["Universidad"]), F.when(F.rand() > 0.4, "Sí").otherwise("No"))
        .otherwise(F.when(F.rand() > 0.6, "Sí").otherwise("No")),
    "ANT_LAB_MES": (F.rand() * 480).cast("int"),
    "ING_MEN_BASE":
        F.when(F.col("nivel_edu").isin(EducacionBasica), ((F.rand() + 0.8) * 8000).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), ((F.rand() + 0.7) * 13000).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]), ((F.rand() + 0.5) * 20000).cast("double"))
        .otherwise(((F.rand() + 0.5) * 40000).cast("double")),
    "GAS_MEN_FIJOS":
        F.when(F.col("nivel_edu").isin(EducacionBasica), ((F.rand() + 0.6) * 8000).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), ((F.rand() + 0.5) * 13000).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]), ((F.rand() + 0.3) * 20000).cast("double"))
        .otherwise(((F.rand() + 0.3) * 40000).cast("double")),
    "ING_ANUAL":
        F.when(F.col("nivel_edu").isin(EducacionBasica),
               (F.col("ingreso_mensual") * 12 + (F.rand() + 1) * 10000).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]),
              (F.col("ingreso_mensual") * 12 + (F.rand() + 1) * 15000).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]),
              (F.col("ingreso_mensual") * 12 + (F.rand() + 1) * 25000).cast("double"))
        .otherwise((F.col("ingreso_mensual") * 12 + (F.rand() + 1) * 50000).cast("double")),
    "GAS_ANUAL":
        F.when(F.col("nivel_edu").isin(EducacionBasica),
               (F.col("gasto_mensual") * 12 + (F.rand() + 0.5) * 10000).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]),
              (F.col("gasto_mensual") * 12 + (F.rand() + 0.5) * 15000).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]),
              (F.col("gasto_mensual") * 12 + (F.rand() + 0.5) * 25000).cast("double"))
        .otherwise((F.col("gasto_mensual") * 12 + (F.rand() + 0.5) * 50000).cast("double")),
    "PATR_EST":
        F.when(F.col("nivel_edu").isin(EducacionBasica), ((F.rand() * 5) * 100000).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), ((F.rand() * 5) * 200000).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]), ((F.rand() * 5) * 400000).cast("double"))
        .otherwise(((F.rand() * 5) * 1000000).cast("double")),
    "SCO_INI":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.rand() * 100 + 300).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.rand() * 200 + 400).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.rand() * 300 + 450).cast("double"))
        .otherwise((F.rand() * 500 + 450).cast("double")),
    "SCO_ACT":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.col("SCO_INI") * (F.rand() * 3)).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.col("SCO_INI") * (F.rand() * 2)).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.col("SCO_INI") * (F.rand() * 1.5)).cast("double"))
        .otherwise((F.col("SCO_INI") * (F.rand() * 1.5)).cast("double")),

    "NUM_CREDACTI":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.rand() * 2).cast("int"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.rand() * 3).cast("int"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.rand() * 6).cast("int"))
        .otherwise((F.rand() * 10).cast("int")),
    "NUM_CUENTAS":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.rand() * 1).cast("int"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.rand() * 2).cast("int"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.rand() * 3).cast("int"))
        .otherwise((F.rand() * 5).cast("int")),
    "NUM_TC":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.rand() * 1).cast("int"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.rand() * 3).cast("int"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.rand() * 5).cast("int"))
        .otherwise((F.rand() * 7).cast("int")),

    # --- Comportamiento de Crédito (Buró) ---
    "SAL_TOTDEU":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.rand() * 2 * 100000).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.rand() * 2 * 180000).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.rand() * 2 * 300000).cast("double"))
        .otherwise((F.rand() * 2000000).cast("double")),
    "UTIL_TC":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.rand() * 1.2).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.rand() * 4).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.rand() * 10).cast("double"))
        .otherwise((F.rand() * 20).cast("double")),
    "MONTO_TOTSOL":
        F.when(F.col("nivel_edu").isin(EducacionBasica), (F.rand() * 20000).cast("double"))
        .when(F.col("nivel_edu").isin(["Bachiller"]), (F.rand() * 50000).cast("double"))
        .when(F.col("nivel_edu").isin(["Universidad"]), (F.rand() * 200000).cast("double"))
        .otherwise((F.rand() * 1000000).cast("double")),
    "MAXDIAS_MORAHIST": (F.rand() * 120).cast("int"),
    "plazo_meses":
        F.element_at(F.array([F.lit(12), F.lit(24), F.lit(36), F.lit(48), F.lit(60)]), (F.rand() * 5 + 1).cast("int")),
    "TASA_INTASIG": F.rand() * 0.35,

})

df_nuevo_lote = df_nuevo_lote.drop("ING_MEN_BASE", "GAS_MEN_FIJOS")
#df_nuevo_lote.repartition(30).write.mode("overwrite").parquet("data/lote_clientes_temp.parquet")  #.partitionBy("estado")
#df_nuevo_lote =  SparkLote.read.parquet("data/lote_clientes_temp.parquet")

df_POLOS =  SparkLote.read.parquet("data/Polos.parquet")
df_match = df_nuevo_lote.crossJoin(F.broadcast(df_POLOS))

df_distancias = df_match.withColumn(
    "dist_info",
    F.struct(
        calculate_distance_O(F.col("lat_cli"), F.col("lon_cli"), F.col("Lat_polo"), F.col("Lon_polo")).alias("d"),
        F.col("FAC_SOCECO").alias("w"),
        F.col("VAL_FRIC").alias("f")
    )
)

# 4. El "Truco" de Agregación: Obtener el Top 3 sin Window Functions
# Agrupamos por cliente y recolectamos todas las distancias en una lista,
# la ordenamos y cortamos a los primeros 3 elementos.
df_top3 = df_distancias.groupBy("id_cliente", "ingreso_mensual", "gasto_mensual"
                                ).agg(F.slice(F.array_sort(F.collect_list("dist_info")), 1, 3).alias("top_3_polos"))

# 5. Aplicación de la fórmula con variables dinámicas

df_final = (df_top3.withColumn(
    "plus_economico",
    F.aggregate(
        # Usamos x.w (FAC_SOCECO) y x.f (VAL_FRIC) de cada polo individualmente
        F.transform("top_3_polos", lambda x: x.w / F.pow(1 + x.d, x.f)),
        F.lit(0.0),
        lambda acc, x: acc + x
    ))
            .withColumn("ingreso_final", F.col("ingreso_mensual") * (1 + F.col("plus_economico"))))

df_final = df_final.select("id_cliente", "plus_economico")
df_unificado = df_nuevo_lote.join(df_final, on="id_cliente", how="left")
df_unificado = df_unificado.fillna({"plus_economico": 0})

#df_nuevo_lote = df_unificado.checkpoint()
#df_unificado.repartition(30).write.mode("overwrite").parquet("data/lote_clientes_temp.parquet")  #.partitionBy("estado")
#df_nuevo_lote =  SparkLote.read.parquet("data/lote_clientes_temp.parquet")
df_nuevo_lote = df_unificado.withColumns({
    "lat_cli": F.round(F.col("lat_cli"), 6),
    "lon_cli": F.round(F.col("lon_cli"), 6),
    "ingreso_mensual": F.round(F.col("ingreso_mensual") * (1 + F.col("plus_economico")), 2),
    "gasto_mensual": F.round(F.col("gasto_mensual") * (1 + F.col("plus_economico")), 2),
    "ING_ANUAL": F.round(F.col("ING_ANUAL") * (1 + F.col("plus_economico")), 2),
    "GAS_ANUAL": F.round(F.col("GAS_ANUAL") * (1 + F.col("plus_economico")), 2),
    "PATR_EST": F.round(F.col("PATR_EST") * (1 + F.col("plus_economico")), 2),
    "SCO_INI": F.round(F.col("SCO_INI"), 2),
    "SCO_ACT": F.round(F.col("SCO_ACT"), 2),
    "SAL_TOTDEU": F.round(F.col("SAL_TOTDEU") * (1 + F.col("plus_economico")), 2),
    "UTIL_TC": F.round(F.col("UTIL_TC"), 2),
    "MONTO_TOTSOL": F.round(F.col("MONTO_TOTSOL") * (1 + F.col("plus_economico")), 2),
    "RAT_DEUDING": F.round(F.col("gasto_mensual") / (F.col("ingreso_mensual") * 12), 4),
    "TASA_INTASIG": F.round(F.col("TASA_INTASIG"), 4),
    "capacidad_ahorro": F.round(F.col("capacidad_ahorro") * (1 + F.col("plus_economico")), 4)

})

df_nuevo_lote.repartition(720, "id_cliente").write.mode("overwrite").parquet("data/lote_clientes_temp.parquet")  #.partitionBy("estado")

schema_historial = StructType([
    StructField("id_cliente", LongType(), True),        StructField("fecha", StringType(), True),
    StructField("mes_simulacion", IntegerType(), True), StructField("plazo_remanente", IntegerType(), True),
    StructField("producto", StringType(), True),        StructField("saldo", DoubleType(), True),
    StructField("pago", DoubleType(), True),            StructField("mora", IntegerType(), True),
    StructField("estatus", StringType(), True),         StructField("score", IntegerType(), True),
    StructField("num_renovacion", IntegerType(), True)
])
from pyspark.sql import functions as F
from pyspark.sql.types import *

schema_historial_pro = StructType([
    StructField("id_cliente", LongType(), True), StructField("producto", StringType(), True),
    StructField("fecha", StringType(), True),    StructField("mes_simulacion", IntegerType(), True),
    StructField("saldo", DoubleType(), True),    StructField("pago", DoubleType(), True),
    StructField("mora", IntegerType(), True),    StructField("estatus", StringType(), True),
    StructField("score_evolutivo", IntegerType(), True),
    # --- Nuevas Variables de Realismo ---
    StructField("num_depositos_aml", IntegerType(), True),
    StructField("pct_cash_aml", DoubleType(), True),
    StructField("dist_trans_hogar", DoubleType(), True),
    StructField("es_anomalia", IntegerType(), True)
])

schema_historial_final = StructType([
    StructField("id_cliente", LongType(), True),    StructField("producto", StringType(), True),
    StructField("fecha", StringType(), True),    StructField("mes_simulacion", IntegerType(), True),
    StructField("saldo", DoubleType(), True),    StructField("pago", DoubleType(), True),
    StructField("mora_shock", IntegerType(), True),    StructField("estatus_base", StringType(), True),
    StructField("score_evolutivo", IntegerType(), True),
    StructField("num_depositos_aml", IntegerType(), True),    # Smurfing
    StructField("pct_cash_aml", DoubleType(), True),# Lavado de dinero
    StructField("dist_trans_hogar", DoubleType(), True),    # Fraude (Geolocalización)
    StructField("es_anomalia", IntegerType(), True),# Target para ML (0: Normal, 1: Fraude, 2: AML)
    StructField("meses_para_liquidar", IntegerType(), True)
])



# 1. Cargar tus datos base (Silver)
df_clientes = SparkLote.read.parquet("data/lote_clientes_temp.parquet")
#df_clientes = df_clientes.repartition(360, "id_cliente")

# 2. Llamar al orquestador
# (Nota: Esta función ya contiene el groupby y el applyInPandas adentro)

df_historial_spark = generar_historial_total_spark(df_clientes, schema = schema_historial_final, sparkSesion = SparkLote, parts=1440)
df_historial_spark = df_historial_spark.repartition(720, "id_cliente")
df_historial_spark = df_historial_spark.withColumn( "dist_trans_hogar", F.when( F.col("producto").isin("Nomina", "Personal"), 0.0  # Forzamos a cero si es uno de estos dos
    ).otherwise( F.col("dist_trans_hogar")  # Mantenemos el valor original para TDC
    ))

# 3. Acciones y Persistencia
# Al ser un DataFrame de Spark, nada se ejecuta hasta que pides el show o el write
df_historial_spark = df_historial_spark.orderBy(    F.col("id_cliente").asc(), F.col("producto").asc(), F.col("fecha").desc() )
df_historial_spark.repartition(720, "id_cliente").write.mode("overwrite").parquet("data/lote_hist_temp.parquet")  #.partitionBy("estado")

SparkLote.stop()



SparkDelta = SparkSession.builder.appName("Export2Delta").master("local[12]") \
    .config("spark.driver.memory", "60g") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

df_cliente_delta =  SparkDelta.read.parquet("data/lote_clientes_temp.parquet")
df_historial_delta =  SparkDelta.read.parquet("data/lote_hist_temp.parquet")

ClientDeltaPATH = r"C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/datalake/clientes_delta"
HistorDeltaPATH = r"C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/datalake/historial_delta"


df_cliente_delta.write.format("delta").mode("append").save(ClientDeltaPATH)
df_historial_delta.write.format("delta").mode("append").save(HistorDeltaPATH)

df_cliente_delta =  SparkDelta.read.parquet("data/lote_clientes_temp.parquet")
df_historial_delta =  SparkDelta.read.parquet("data/lote_hist_temp.parquet")
print(df_cliente_delta.count())
print(df_historial_delta.count())

SparkDelta.stop()





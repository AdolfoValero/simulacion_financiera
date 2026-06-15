
import pandas as pd
import numpy as np
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import *
import pyspark

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SimulacionFinanciera") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "20") \
    .config("spark.python.worker.timeout", "600") \
    .getOrCreate()


def generar_y_guardar_pudf(pdf_clientes):
    # IMPORTS DENTRO PARA EVITAR ERRORES DE SERIALIZACIÓN
    import pandas as pd
    import random
    import uuid
    import os
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    historial_list = []
    fecha_base = datetime.strptime("2020-01-01", "%Y-%m-%d")


    productos_cfg = {
        "Nomina": {"tasa_anual": 0.22, "plazo": 24, "lim_ing": 1.5, "tipo": "fijo"},
        "Personal": {"tasa_anual": 0.38, "plazo": 36, "lim_ing": 2.2, "tipo": "fijo"},
        "TDC_1": {"tasa_anual": 0.55, "plazo": 12, "lim_ing": 0.9, "tipo": "revolvente"},
        "TDC_2": {"tasa_anual": 0.75, "plazo": 12, "lim_ing": 1.4, "tipo": "revolvente"}
    }

    for _, cliente in pdf_clientes.iterrows():
        id_act = int(cliente['id_cliente'])
        ingreso = float(cliente['ingreso_mensual'])
        score_evolutivo = int(cliente['SCO_ACT'])
        mes_inicio_cliente = random.randint(0, 24)

        prods_usuario = [p for p in productos_cfg.keys() if random.random() < (0.45 if ingreso > 12000 else 0.18)]

        for prod in prods_usuario:
            cfg = productos_cfg[prod]
            plazo_fijo = cfg.get('plazo', 24)
            meses_remanentes = plazo_fijo
            tasa_ms = cfg['tasa_anual'] / 12
            monto_u = ingreso * cfg['lim_ing'] * random.uniform(0.6, 1.4)
            saldo = monto_u
            pago_base = (monto_u * (1 + cfg['tasa_anual'])) / plazo_fijo
            mora_acumulada = 0

            for m in range(1, 49):
                if m < mes_inicio_cliente: continue
                fecha_mov = (fecha_base + relativedelta(months=m)).strftime("%Y-%m-%d")

                # Lógica de Mora
                prob_mora = 0.03 if score_evolutivo > 720 else 0.12 if score_evolutivo > 580 else 0.45
                dias_mora_mes = 0
                if random.random() < prob_mora:
                    dias_mora_mes = random.choice([5, 15, 30, 45, 60, 90])
                    mora_acumulada = max(mora_acumulada, dias_mora_mes)
                    score_evolutivo -= (dias_mora_mes // 3)
                else:
                    mora_acumulada = max(0, mora_acumulada - 30)
                    score_evolutivo += random.randint(1, 4)

                score_evolutivo = max(300, min(850, score_evolutivo))
                estatus = "Vigente" if mora_acumulada < 90 else "Cobranza"

                # Lógica Financiera
                interes_mes = saldo * (tasa_ms if estatus == "Vigente" else 0.05)
                if estatus == "Vigente":
                    if cfg['tipo'] == "revolvente":
                        meses_remanentes = plazo_fijo if random.random() < 0.7 else max(0, meses_remanentes - 1)
                        gasto_tarjeta = monto_u * random.uniform(0.05, 0.3)
                        pago_efectuado = (gasto_tarjeta + interes_mes) if score_evolutivo > 690 else (
                                                                                                                 gasto_tarjeta + interes_mes) * random.uniform(
                            0.1, 0.4)
                        if dias_mora_mes >= 30: pago_efectuado = 0
                        saldo = max(0, (saldo + gasto_tarjeta + interes_mes) - pago_efectuado)
                    else:
                        pago_efectuado = pago_base if dias_mora_mes < 30 else 0
                        saldo = max(0, (saldo + interes_mes) - pago_efectuado)
                        meses_remanentes = max(0, meses_remanentes - 1)
                        if (meses_remanentes == 0 or random.random() < 0.05) and saldo < (monto_u * 0.3):
                            saldo += (monto_u * 0.8);
                            meses_remanentes = plazo_fijo;
                            estatus = "Renovado"
                else:
                    pago_efectuado = saldo * 0.02 if random.random() < 0.1 else 0
                    saldo = saldo + interes_mes - pago_efectuado
                    meses_remanentes = max(0, meses_remanentes - 1)

                historial_list.append({
                    "id_cliente": id_act, "fecha": str(fecha_mov), "mes_simulacion": int(m),
                    "plazo_remanente": int(meses_remanentes), "producto": str(prod),
                    "saldo": float(saldo), "pago": float(pago_efectuado),
                    "mora": int(dias_mora_mes), "estatus": str(estatus), "score": int(score_evolutivo)
                })
                if saldo <= 0 and cfg['tipo'] == "fijo" and estatus != "Renovado": break

    # Verificación de datos antes de convertir
    if not historial_list:
        return pd.DataFrame(columns=['id_cliente', 'fecha', 'mes_simulacion', 'plazo_remanente',
                                     'producto', 'saldo', 'pago', 'mora', 'estatus', 'score'])

    df_temp = pd.DataFrame(historial_list)

    # CASTING MANUAL (Evita errores de tipos entre PyArrow y Spark)
    df_temp['id_cliente'] = df_temp['id_cliente'].astype('int64')
    df_temp['mes_simulacion'] = df_temp['mes_simulacion'].astype('int32')
    df_temp['plazo_remanente'] = df_temp['plazo_remanente'].astype('int32')
    df_temp['mora'] = df_temp['mora'].astype('int32')
    df_temp['score'] = df_temp['score'].astype('int32')

    path_temp = "C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/tempo/"
    os.makedirs(path_temp, exist_ok=True)

    archivo = os.path.join(path_temp, f"lote_{uuid.uuid4().hex}.parquet")
    df_temp.to_parquet(archivo, index=False)

    return df_temp.head(1)


# 1. Definimos el esquema de salida para Spark
schema_historial = StructType([
    StructField("id_cliente", LongType(), True),
    StructField("fecha", StringType(), True),
    StructField("mes_simulacion", IntegerType(), True),
    StructField("plazo_remanente", IntegerType(), True),
    StructField("producto", StringType(), True),
    StructField("saldo", DoubleType(), True),
    StructField("pago", DoubleType(), True),
    StructField("mora", IntegerType(), True),
    StructField("estatus", StringType(), True),
    StructField("score", IntegerType(), True)
])

# Convierte por lotes pequeños usando un iterador local
clientes_locales = df_nuevo_lote.repartition(20).toLocalIterator()

for pdf_chunk in clientes_locales:
    # pdf_chunk es una fila de Spark, la procesas manualmente o por grupos
    generar_y_guardar_pudf(pd.DataFrame([pdf_chunk.asDict()]))


# 1. Reparticionamos para que cada worker tenga una carga ligera
df_procesamiento = df_nuevo_lote.repartition(100).groupby("id_cliente").applyInPandas(
    generar_y_guardar_pudf,
    schema=schema_historial
)

# 2. El comando .collect() ahora solo servirá para ACTIVAR el guardado en disco
print("Iniciando escritura en disco desde workers...")
df_procesamiento.collect()

# 3. Leemos el resultado real desde la carpeta donde los workers escribieron
df_historial_final = spark.read.parquet("C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/tempo/*.parquet")

print(f"Procesamiento completado. Registros totales: {df_historial_final.count()}")




df_reparticionado = df_nuevo_lote.repartition(500, "id_cliente")
df_historial_spark = df_nuevo_lote.groupby("id_cliente").applyInPandas( generar_historial_pudf, schema=schema_historial)
df_historial_spark.repartition(30).write.mode("overwrite").parquet("data/lote_hist_temp.parquet")#.partitionBy("estado")



df_historial_spark.show()


# 3. Guardar (Append)
#df_nuevo_lote.write.format("delta").mode("append").save(PATH_DATALAKE)
tabla_clientes = spark.read.parquet("data/lote_clientes_temp.parquet")
tabla_clientes.show()




df_lote.write.format("delta").mode("append").save(PATH_DATALAKE)

print(f"Lote guardado en {PATH_DATALAKE}. Registros totales: {spark.read.format('delta').load(PATH_DATALAKE).count()}")

# 4. EJEMPLOS DE TIME TRAVEL Y ANÁLISIS
print("\n--- HISTORIAL DE LA TABLA ---")
dt = DeltaTable.forPath(spark, PATH_DATALAKE)
dt.history().select("version", "timestamp", "operation").show()

# Supongamos que queremos comparar la Versión Actual vs la Versión 0 (Inicial)
version_actual = dt.history().select(F.max("version")).collect()[0][0]

if version_actual > 0:
    print(f"Comparando Versión {version_actual} contra Versión 0...")

    df_v_actual = spark.read.format("delta").load(PATH_DATALAKE)
    df_v_0 = spark.read.format("delta").option("versionAsOf", 0).load(PATH_DATALAKE)

    # Análisis de consistencia: Sueldo promedio por versión
    avg_actual = df_v_actual.select(F.avg("sueldo_mensual")).collect()[0][0]
    avg_v0 = df_v_0.select(F.avg("sueldo_mensual")).collect()[0][0]

    print(f"Sueldo Promedio Actual (v{version_actual}): ${avg_actual:,.2f}")
    print(f"Sueldo Promedio Inicial (v0): ${avg_v0:,.2f}")
else:
    print("Esta es la primera carga (v0). Ejecuta el script de nuevo para hacer append y ver el Time Travel.")

df_v_actual.show(5)



# 1. Cargar la tabla Delta existente (Metadatos)
dt = DeltaTable.forPath(spark, PATH_DATALAKE)

# 2. Supongamos que generamos un pequeño lote de "Actualizaciones"
# Vamos a tomar 5 clientes existentes y cambiarles el sueldo y el empleo
df_actualizaciones = spark.read.format("delta").load(PATH_DATALAKE).limit(5)

df_actualizaciones = df_actualizaciones.withColumn("sueldo_mensual", F.col("sueldo_mensual") * 1.10) \
                                       .withColumn("empresa", F.lit("Empresa Promovida S.A."))

# 3. Ejecutar el MERGE (Upsert)
print("Ejecutando MERGE: Actualizando sueldos de clientes existentes...")

dt.alias("historico") \
  .merge(
    df_actualizaciones.alias("nuevos"),
    "historico.id_cliente = nuevos.id_cliente"  # Condición de cruce
  ) \
  .whenMatchedUpdate(set = {
    "sueldo_mensual": "nuevos.sueldo_mensual",
    "empresa": "nuevos.empresa"
  }) \
  .whenNotMatchedInsertAll() \
  .execute()

# 4. Verificar el cambio y el historial
print("\n--- NUEVO HISTORIAL DESPUÉS DEL MERGE ---")
dt.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# Ver que los cambios se aplicaron
spark.read.format("delta").load(PATH_DATALAKE) \
     .filter(F.col("empresa") == "Empresa Promovida S.A.") \
     .select("id_cliente", "nombre", "empresa", "sueldo_mensual").show()












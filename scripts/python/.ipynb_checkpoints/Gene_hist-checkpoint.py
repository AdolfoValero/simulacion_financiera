import pandas as pd
import numpy as np
import random
import uuid
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import builtins  # <--- Agrega este import al inicio del archivo


def calculate_distance_O(lat1, lon1, lat2, lon2, threshold_km=10000000.0):
    from pyspark.sql import functions as F
    inner_val = (
            F.sin(F.radians(lat1)) * F.sin(F.radians(lat2)) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) *
            F.cos(F.radians(lon2) - F.radians(lon1))
    )
    clipped_val = F.greatest(F.lit(-1.0), F.least(F.lit(1.0), inner_val))
    distancia = F.acos(clipped_val) * F.lit(6371.0)
    return F.when(distancia <= threshold_km, distancia).otherwise(F.lit(None))


def generar_lote_clientes_Spark(n_clientes, df_prob, df_salarios, id_inicial=1):
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DoubleType, DateType
    import pandas as pd

    # Recuperamos la sesión activa de Spark
    spark = SparkSession.builder.getOrCreate()

    # --- CORRECCIÓN CRÍTICA: Conversión de Pandas a Spark ---
    # Esto elimina el AttributeError: 'DataFrame' object has no attribute 'withColumn'
    if isinstance(df_prob, pd.DataFrame):
        df_prob = spark.createDataFrame(df_prob)

    if isinstance(df_salarios, pd.DataFrame):
        df_salarios = spark.createDataFrame(df_salarios)

    # --- PREPARACIÓN DE DATOS ---
    df_trabajo = df_prob
    df_trabajo = df_trabajo.withColumn('cantidad', (F.lit(n_clientes) * F.col('prob')).cast('int'))

    # Join para consolidar la receta de generación
    df_gen = df_trabajo.join(df_salarios, on="cve_ent", how="left").filter("cantidad > 0")

    # Esquema de salida (Mantenido según tu definición original)
    schema = StructType([
        StructField("id_cliente", LongType(), True),
        StructField("h3_index_res8", StringType(), True),
        StructField("cve_ent", IntegerType(), True),
        StructField("cve_mun", IntegerType(), True),
        StructField("lat_cli", DoubleType(), True),
        StructField("lon_cli", DoubleType(), True),
        StructField("nombre", StringType(), True),
        StructField("sexo", StringType(), True),
        StructField("edad", IntegerType(), True),
        StructField("Fecha_nacimiento", DateType(), True),
        StructField("estado_civil", StringType(), True),
        StructField("numero_de_hijos", IntegerType(), True),
        StructField("nivel_edu", StringType(), True),
        StructField("ingreso_mensual", DoubleType(), True),
        StructField("gasto_mensual", DoubleType(), True),
        StructField("capacidad_ahorro", DoubleType(), True),
        StructField("empresa", StringType(), True),
        StructField("Empleo", StringType(), True),
        StructField("ciudad", StringType(), True),
        StructField("Telefono", StringType(), True),
        StructField("email", StringType(), True),
        StructField("zip", StringType(), True)
    ])

    # --- LÓGICA DISTRIBUIDA (Mantenida íntegra) ---
    def ejecutar_generacion(pdf_iterator):
        import h3
        import numpy as np
        import pandas as pd
        import random
        from faker import Faker

        fake = Faker('es_MX')
        # ... (Tu diccionario de prefijos_cp, mults y niveles se mantiene igual)
        prefijos_cp = {1: "20", 2: "21", 3: "23", 4: "24", 5: "25", 6: "28", 7: "29", 8: "31",
                       9: "0", 10: "34", 11: "36", 12: "39", 13: "42", 14: "44", 15: "50", 16: "58",
                       17: "62", 18: "63", 19: "64", 20: "68", 21: "72", 22: "76", 23: "77", 24: "78",
                       25: "80", 26: "83", 27: "86", 28: "87", 29: "90", 30: "91", 31: "97", 32: "98"}
        mults = {"Sin Educacion": 0.5, "Primaria": 0.7, "Secundaria": 0.85, "Preparatoria": 1.0,
                 "Licenciatura": 1.8, "Maestría": 2.5, "Doctorado": 3.5}
        niveles = ["Sin Educacion", "Primaria", "Secundaria", "Preparatoria", "Licenciatura", "Maestría", "Doctorado"]

        sigma_geo = 0.01

        for df_segmento in pdf_iterator:
            lista_clientes = []
            for row in df_segmento.itertuples():
                factor_b = row.salario_base
                lat_m, lon_m = row.lat_mun, row.lon_mun
                cve_ent, cve_mun = int(row.cve_ent), int(row.cve_mun)
                prefijo = prefijos_cp.get(cve_ent, "0")

                for _ in range(row.cantidad):
                    sexo = random.choice(['M', 'F'])
                    edad = random.randint(18, 75)
                    educ = random.choices(niveles[:4], weights=[1, 3, 4, 7, 2, 1, 1][:4], k=1)[0] if edad < 21 else random.choices(niveles, weights=[1, 3, 4, 7, 2, 1, 1], k=1)[0]


                    b_ajustado = factor_b * mults.get(educ, 1.0)

                    # Fórmulas de ingreso y gasto
                    ingreso = b_ajustado * pow(pow(random.random(), -1 / 0.7) - 1, -1 / 3.5)
                    gasto_ini = (b_ajustado * 0.61) * pow(pow(random.random(), -1 / 0.8) - 1, -1 / 4.0)
                    gasto_fin = min(gasto_ini, ingreso * random.uniform(0.8, 1.2))

                    lat_cli = np.random.normal(lat_m, sigma_geo)
                    lon_cli = np.random.normal(lon_m, sigma_geo)
                    h3_res8 = h3.latlng_to_cell(lat_cli, lon_cli, 8)
                    cp = prefijo + "".join([str(random.randint(0, 9)) for _ in range(5 - len(prefijo))])
                    lista_clientes.append({
                        "id_cliente": 0,  # Placeholder
                        "h3_index_res8": h3_res8, "cve_ent": cve_ent, "cve_mun": cve_mun,
                        "lat_cli": float(lat_cli), "lon_cli": float(lon_cli),
                        "nombre": fake.name_male() if sexo == 'M' else fake.name_female(),
                        "sexo": sexo, "edad": edad,
                        "Fecha_nacimiento": fake.date_of_birth(minimum_age=edad, maximum_age=edad),
                        "estado_civil": random.choices(["Soltero(a)", "Casado(a)", "Divorciado(a)", "Viudo(a)"], weights=[3, 4, 2, 1], k=1)[0],
                        "numero_de_hijos": random.choices([0,1,2,3,4,5], weights=[5, 3, 2, 1, 1, 1], k=1)[0] if edad > 16 else 0,
                        "nivel_edu": educ, "ingreso_mensual": float(ingreso),
                        "gasto_mensual": float(gasto_fin), "capacidad_ahorro": float(max(0, ingreso - gasto_fin)),
                        "empresa": fake.company(), "Empleo": fake.job(),
                        "ciudad": getattr(row, 'nom_mun', "Ciudad Desconocida"),
                        "Telefono": fake.phone_number(), "email": fake.email(), "zip": cp
                    })
            yield pd.DataFrame(lista_clientes)

    # --- EJECUCIÓN ---
    # Reparticionamos para balancear la carga del millón de registros
    df_resultado = df_gen.repartition(240).mapInPandas(ejecutar_generacion, schema=schema)

    # Asignación de ID Secuencial Global
    from pyspark.sql.window import Window
    w = Window.orderBy(F.monotonically_increasing_id())
    df_resultado = df_resultado.withColumn("id_cliente", F.row_number().over(w) + (id_inicial - 1))

    return df_resultado












def generar_lote_clientes_SparkDisk(n_clientes, df_prob, df_salarios, id_inicial=1, salida=""):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DoubleType, DateType
    import pandas as pd

    # --- PREPARACIÓN DE DATOS EN SPARK ---
    # Asumimos que df_prob y df_salarios entran como Spark DataFrames
    df_trabajo = df_prob
    df_trabajo = df_trabajo.withColumn('cantidad', (F.lit(n_clientes) * F.col('prob')).cast('int'))

    # Realizamos el Join en Spark (equivalente al merge de pandas)
    df_gen = df_trabajo.join(df_salarios, on="cve_ent", how="left").filter("cantidad > 0")

    # Definición del esquema exacto para recibir los datos de los workers
    schema = StructType([
        StructField("id_cliente", LongType(), True),
        StructField("h3_index_res8", StringType(), True),
        StructField("cve_ent", IntegerType(), True),
        StructField("cve_mun", IntegerType(), True),
        StructField("lat_cli", DoubleType(), True),
        StructField("lon_cli", DoubleType(), True),
        StructField("nombre", StringType(), True),
        StructField("sexo", StringType(), True),
        StructField("edad", IntegerType(), True),
        StructField("Fecha_nacimiento", DateType(), True),
        StructField("estado_civil", StringType(), True),
        StructField("numero_de_hijos", IntegerType(), True),
        StructField("nivel_edu", StringType(), True),
        StructField("ingreso_mensual", DoubleType(), True),
        StructField("gasto_mensual", DoubleType(), True),
        StructField("capacidad_ahorro", DoubleType(), True),
        StructField("empresa", StringType(), True),
        StructField("Empleo", StringType(), True),
        StructField("ciudad", StringType(), True),
        StructField("Telefono", StringType(), True),
        StructField("email", StringType(), True),
        StructField("zip", StringType(), True)
    ])

    # --- FUNCIÓN INTERNA PARA LOS HILOS DEL RYZEN ---
    def ejecutar_generacion(pdf_iterator):
        import h3
        import numpy as np
        import pandas as pd
        import random
        from faker import Faker

        fake = Faker('es_MX')

        prefijos_cp = {
            1: "20", 2: "21", 3: "23", 4: "24", 5: "25", 6: "28", 7: "29", 8: "31",
            9: "0", 10: "34", 11: "36", 12: "39", 13: "42", 14: "44", 15: "50", 16: "58",
            17: "62", 18: "63", 19: "64", 20: "68", 21: "72", 22: "76", 23: "77", 24: "78",
            25: "80", 26: "83", 27: "86", 28: "87", 29: "90", 30: "91", 31: "97", 32: "98"
        }

        mults = {"Sin Educacion": 0.5, "Primaria": 0.7, "Secundaria": 0.85, "Preparatoria": 1.0,
                 "Licenciatura": 1.8, "Maestría": 2.5, "Doctorado": 3.5}
        niveles = ["Sin Educacion", "Primaria", "Secundaria", "Preparatoria", "Licenciatura", "Maestría", "Doctorado"]
        sigma_geo = 0.01

        for df_segmento in pdf_iterator:
            lista_clientes = []  # Mantenemos nombre
            for row in df_segmento.itertuples():
                factor_b = row.salario_base
                lat_m = row.lat_mun
                lon_m = row.lon_mun
                cve_ent = int(row.cve_ent)
                cve_mun = int(row.cve_mun)
                prefijo = prefijos_cp.get(cve_ent, "0")

                for _ in range(row.cantidad):
                    sexo = random.choice(['M', 'F'])
                    edad = random.randint(18, 75)
                    educ = random.choice(niveles[:3]) if edad < 21 else random.choice(niveles)
                    b_ajustado = factor_b * mults.get(educ, 1.0)

                    u_ing = random.random()
                    ingreso = b_ajustado * pow(pow(u_ing, -1 / 0.7) - 1, -1 / 3.5)

                    u_gas = random.random()
                    gasto_inicial = (b_ajustado * 0.61) * pow(pow(u_gas, -1 / 0.8) - 1, -1 / 4.0)

                    limite_gasto = ingreso * random.uniform(0.8, 1.2)
                    gasto_final = min(gasto_inicial, limite_gasto)
                    ahorro = max(0, ingreso - gasto_final)

                    lat_cli = np.random.normal(lat_m, sigma_geo)
                    lon_cli = np.random.normal(lon_m, sigma_geo)

                    h3_res8 = h3.latlng_to_cell(lat_cli, lon_cli, 8)
                    digitos_faltantes = 5 - len(prefijo)
                    cp_falso = prefijo + "".join([str(random.randint(0, 9)) for _ in range(digitos_faltantes)])

                    id_actual = 0  # El ID se calculará globalmente al final para evitar colisiones

                    lista_clientes.append({
                        "id_cliente": id_actual,
                        "h3_index_res8": h3_res8,
                        "cve_ent": cve_ent,
                        "cve_mun": cve_mun,
                        "lat_cli": float(lat_cli),
                        "lon_cli": float(lon_cli),
                        "nombre": fake.name_male() if sexo == 'M' else fake.name_female(),
                        "sexo": sexo,
                        "edad": edad,
                        "Fecha_nacimiento": fake.date_of_birth(minimum_age=edad, maximum_age=edad),
                        "estado_civil": random.choice(["Soltero(a)", "Casado(a)", "Divorciado(a)", "Viudo(a)"]),
                        "numero_de_hijos": random.randint(0, 5) if edad > 22 else 0,
                        "nivel_edu": educ,
                        "ingreso_mensual": float(ingreso),
                        "gasto_mensual": float(gasto_final),
                        "capacidad_ahorro": float(ahorro),
                        "empresa": fake.company(),
                        "Empleo": fake.job(),
                        "ciudad": getattr(row, 'nom_mun', "Ciudad Desconocida"),
                        "Telefono": fake.phone_number(),
                        "email": fake.email(),
                        "zip": cp_falso
                    })
            yield pd.DataFrame(lista_clientes)

    # --- EJECUCIÓN DISTRIBUIDA ---
    # Reparticionamos para que todos los hilos trabajen (240 tareas para monitoreo fluido)
    df_resultado = df_gen.repartition(240).mapInPandas(ejecutar_generacion, schema=schema)

    # Asignación de ID Global Secuencial (Respetando id_inicial)
    from pyspark.sql.window import Window
    w = Window.orderBy(F.monotonically_increasing_id())
    df_resultado = df_resultado.withColumn("id_cliente", F.row_number().over(w) + (id_inicial - 1))

    # Escritura final si se proporciona ruta
    if salida:
        df_resultado.write.mode("overwrite").parquet(salida)
        print(f"Lote guardado exitosamente en: {salida}")

    return df_resultado



def generar_lote_clientes(n_clientes, df_prob, df_salarios, id_inicial=1, salida=""):
    import h3  # Importación interna para evitar errores de serialización en Spark
    from faker import Faker

    fake = Faker('es_MX')
    lista_clientes = []

    prefijos_cp = {
        1: "20", 2: "21", 3: "23", 4: "24", 5: "25", 6: "28", 7: "29", 8: "31",
        9: "0", 10: "34", 11: "36", 12: "39", 13: "42", 14: "44", 15: "50", 16: "58",
        17: "62", 18: "63", 19: "64", 20: "68", 21: "72", 22: "76", 23: "77", 24: "78",
        25: "80", 26: "83", 27: "86", 28: "87", 29: "90", 30: "91", 31: "97", 32: "98"
    }

    df_trabajo = df_prob.copy()
    df_trabajo['cantidad'] = (n_clientes * df_trabajo['prob']).astype(int)
    df_gen = pd.merge(df_trabajo, df_salarios, on="cve_ent", how="left").query("cantidad > 0")

    mults = {"Sin Educacion": 0.5, "Primaria": 0.7, "Secundaria": 0.85, "Preparatoria": 1.0,
             "Licenciatura": 1.8, "Maestría": 2.5, "Doctorado": 3.5}
    niveles = ["Sin Educacion", "Primaria", "Secundaria", "Preparatoria", "Licenciatura", "Maestría", "Doctorado"]
    sigma_geo = 0.01

    for _, row in df_gen.iterrows():
        factor_b = row['salario_base']
        lat_m = row['lat_mun']
        lon_m = row['lon_mun']
        cve_ent = int(row['cve_ent'])
        cve_mun = int(row['cve_mun'])
        prefijo = prefijos_cp.get(cve_ent, "0") # Default si no encuentra

        for _ in range(row['cantidad']):
            sexo = random.choice(['M', 'F'])
            edad = random.randint(18, 75)
            educ = random.choice(niveles[:3]) if edad < 21 else random.choice(niveles)
            b_ajustado = factor_b * mults.get(educ, 1.0)

            u_ing = random.random()
            ingreso = b_ajustado * pow(pow(u_ing, -1 / 0.7) - 1, -1 / 3.5)

            u_gas = random.random()
            gasto_inicial = (b_ajustado * 0.61) * pow(pow(u_gas, -1 / 0.8) - 1, -1 / 4.0)

            limite_gasto = ingreso * random.uniform(0.8, 1.2)
            gasto_final = min(gasto_inicial, limite_gasto)
            ahorro = max(0, ingreso - gasto_final)

            # --- COORDENADAS ---
            lat_cli = np.random.normal(lat_m, sigma_geo)
            lon_cli = np.random.normal(lon_m, sigma_geo)

            # --- GEO-INTELIGENCIA H3 ---
            # Generamos el índice hexadecimal de Uber (Res 8: ~0.7 km²)
            h3_res8 = h3.latlng_to_cell(lat_cli, lon_cli, 8)
            digitos_faltantes = 5 - len(prefijo)
            cp_falso = prefijo + "".join([str(random.randint(0, 9)) for _ in range(digitos_faltantes)])
            id_actual = id_inicial + len(lista_clientes)
            lista_clientes.append({
                "id_cliente": id_actual,
                "h3_index_res8": h3_res8,  # Nueva variable de geointeligencia
                "cve_ent": cve_ent,
                "cve_mun": cve_mun,
                "lat_cli": float(lat_cli),
                "lon_cli": float(lon_cli),
                "nombre": fake.name_male() if sexo == 'M' else fake.name_female(),
                "sexo": sexo,
                "edad": edad,
                "Fecha_nacimiento": fake.date_of_birth(minimum_age=edad, maximum_age=edad),
                "estado_civil": random.choice(["Soltero(a)", "Casado(a)", "Divorciado(a)", "Viudo(a)"]),
                "numero_de_hijos": random.randint(0, 5) if edad > 22 else 0,
                "nivel_edu": educ,
                "ingreso_mensual": float(ingreso),
                "gasto_mensual": float(gasto_final),
                "capacidad_ahorro": float(ahorro),
                "empresa": fake.company(),
                #"rama_industria": random.choice(["Tecnología", "Salud", "Finanzas", "Manufactura"]),
                "Empleo": fake.job(),
                "ciudad": row.get('nom_mun', fake.city()),
                "Telefono": fake.phone_number(),
                "email": fake.email(),
                "zip": cp_falso#fake.postcode()

            })
    #return pd.DataFrame(lista_clientes)
    pd.DataFrame(lista_clientes).to_parquet(salida, index=False)
    print(f"Archivo {salida} creado exitosamente con Pandas.")


def generar_historial_total_spark(df_clientes, schema, sparkSesion, parts=100):
    import pyspark.sql.functions as F

    # Grid de meses y productos
    df_prods = sparkSesion.createDataFrame([
        ("Nomina", 0.22, 24, 1.5, "fijo"),
        ("Personal", 0.38, 36, 2.2, "fijo"),
        ("TDC_1", 0.55, 12, 0.9, "revolvente"),
        ("TDC_2", 0.75, 12, 1.4, "revolvente")
    ], ["producto", "tasa_anual", "plazo_fijo", "lim_ing", "tipo_prod"])

    df_grid = df_clientes.crossJoin(df_prods).crossJoin(
        sparkSesion.range(1, 49).withColumnRenamed("id", "mes_simulacion"))

    # Filtro determinista por Hash
    df_struct = df_grid.withColumn("h", F.hash(F.col("id_cliente"), F.col("producto")) % 100 / 100)
    df_struct = df_struct.filter(F.col("h") < F.when(F.col("ingreso_mensual") > 12000, 0.45).otherwise(0.18))

    df_struct = df_struct.withColumns({
        "fecha": F.date_format(F.expr("add_months(to_date('2020-01-01'), mes_simulacion)"), "yyyy-MM-dd"),
        "monto_u_inicial": F.col("ingreso_mensual") * F.col("lim_ing") * (
                    F.hash(F.col("id_cliente")) % 100 / 100 * 0.5 + 0.8),
        "es_anomalia": F.when(F.hash(F.col("id_cliente"), F.lit("f")) % 100 < 1, 1).when(
            F.hash(F.col("id_cliente"), F.lit("a")) % 100 < 2, 2).otherwise(0),
        "num_depositos_aml": (F.hash(F.col("id_cliente"), F.col("mes_simulacion")) % 5 + 1).cast("int"),
        "dist_trans_hogar": (F.hash(F.col("id_cliente"), F.col("mes_simulacion"), F.lit("d")) % 50).cast("double"),
        "pct_cash_aml": (F.hash(F.col("id_cliente"), F.lit("c")) % 100 / 100 * 0.4)
    })

    return df_struct.repartition(parts, "id_cliente").groupBy("id_cliente").applyInPandas(
        simular_core_recursivo, schema=schema
    )


def simular_core_recursivo(pdf_lote):
    import pandas as pd
    import numpy as np

    # 1. Aseguramos el orden cronológico para que la deuda se arrastre correctamente
    pdf_lote = pdf_lote.sort_values(['id_cliente', 'producto', 'mes_simulacion'])
    results = []

    for (id_cli, prod_nom), group in pdf_lote.groupby(['id_cliente', 'producto']):
        # --- Estado Inicial del Crédito ---
        row_ini = group.iloc[0]
        saldo_ant = float(row_ini['monto_u_inicial'])
        score_ant = float(row_ini['SCO_INI'])
        mora_acum = 0
        pago_vencido_capital = 0.0  # Acumulado de capital no pagado
        tasa_ord_ms = float(row_ini['tasa_anual']) / 12
        tasa_mor_ms = tasa_ord_ms * 1.5  # Penalización sobre el monto vencido

        # Cálculo de cuota para préstamos (Amortización Francesa)
        plazo_pactado = int(row_ini['plazo_fijo'])
        cuota_fija = (saldo_ant * tasa_ord_ms) / (1 - (1 + tasa_ord_ms) ** (-plazo_pactado)) if row_ini['tipo_prod'] == 'fijo' else 0

        for row in group.itertuples():
            # --- 2. Determinación de lo que el cliente DEBE pagar (Exigible) ---
            if row.tipo_prod == 'fijo':
                # Cuota normal + (Atrasos con su respectivo interés moratorio)
                exigible_mes = cuota_fija + (pago_vencido_capital * (1 + tasa_mor_ms))
            else:
                # Pago mínimo TDC: Intereses del mes + un porcentaje del capital
                exigible_mes = (saldo_ant * tasa_ord_ms) + (saldo_ant * 0.0125) if saldo_ant > 0 else 0

            # --- 3. Decisión de Pago (Conducta Humana) ---
            # Si ya tiene mora, la probabilidad de seguir fallando sube al 55%
            prob_exito_pago = 0.94 if mora_acum == 0 else 0.45
            pago_realizado = 0.0

            if np.random.random() < prob_exito_pago and saldo_ant > 0:
                # El cliente intenta pagar, pero su bolsillo tiene un límite (45% de su sueldo)
                capacidad_max = row.ingreso_mensual * 0.45
                pago_realizado = min(saldo_ant + (saldo_ant * tasa_ord_ms), exigible_mes, capacidad_max)

                # Si cubrió el exigible, bajamos la mora. Si no, arrastramos la diferencia.
                if pago_realizado >= exigible_mes:
                    mora_acum = max(0, mora_acum - 30)
                    pago_vencido_capital = 0.0
                else:
                    pago_vencido_capital = max(0, exigible_mes - pago_realizado)

                score_ant = min(850, score_ant + np.random.randint(1, 3))
            else:
                # Evento de impago: acumulamos mora y deuda vencida
                if saldo_ant > 0:
                    mora_acum += 30
                    pago_vencido_capital = exigible_mes
                    score_ant = max(300, score_ant - (mora_acum / 4))
                pago_realizado = 0.0

            # --- 4. Actualización del Saldo (La "bola de nieve" financiera) ---
            int_ordinario = saldo_ant * tasa_ord_ms
            gastos_cobranza = 350.0 if mora_acum > 0 else 0.0  # Comisión fija bancaria

            # Gasto en TDC: si es anomalía (fraude), gasta agresivamente
            gasto_extra = 0.0
            if row.tipo_prod == 'revolvente' and saldo_ant < (row.monto_u_inicial * 1.1):
                gasto_extra = row.monto_u_inicial * 0.35 if row.es_anomalia == 1 else row.monto_u_inicial * np.random.uniform(
                    0, 0.08)

            saldo_act = max(0.0, saldo_ant + int_ordinario + gastos_cobranza + gasto_extra - pago_realizado)

            # --- 5. SOLUCIÓN AL LOGARITMO: Proyección de meses para liquidar ---
            # Validamos que el pago realmente reduzca la deuda para evitar errores matemáticos
            interes_generado = saldo_act * tasa_ord_ms

            if saldo_act <= 0:
                meses_liq = 0
            elif pago_realizado <= interes_generado:
                # Si el pago no cubre ni el interés, el crédito nunca se pagará (99 meses)
                meses_liq = 99
            else:
                try:
                    # Aplicamos la fórmula financiera Nper (Número de periodos)
                    # np.log(Pago / (Pago - Saldo * Tasa)) / np.log(1 + Tasa)
                    argumento_log = pago_realizado / (pago_realizado - interes_generado)
                    m_liq = np.log(argumento_log) / np.log(1 + tasa_ord_ms)
                    meses_liq = int(min(99, np.ceil(m_liq)))
                except:
                    meses_liq = 99

            # --- 6. Formateo de salida para Spark ---
            results.append({
                "id_cliente": int(id_cli),
                "producto": str(prod_nom),
                "fecha": str(row.fecha),
                "mes_simulacion": int(row.mes_simulacion),
                "saldo": float(round(saldo_act, 2)),
                "pago": float(round(pago_realizado, 2)),
                "mora_shock": int(mora_acum),
                "estatus_base": "Cobranza" if mora_acum >= 90 else ("Liquidado" if saldo_act <= 0 else "Vigente"),
                "score_evolutivo": int(score_ant),
                "num_depositos_aml": int(row.num_depositos_aml),
                "pct_cash_aml": float(row.pct_cash_aml),
                "dist_trans_hogar": float(row.dist_trans_hogar),
                "es_anomalia": int(row.es_anomalia),
                "meses_para_liquidar": int(meses_liq)
            })

            # El saldo de este mes será el saldo_ant del siguiente
            saldo_ant = saldo_act

    return pd.DataFrame(results)






#import os
#import sys
import importlib
import pandas as pd
#from faker import Faker

from pyspark.sql import SparkSession
import pyspark.sql.functions as F # Usamos alias para evitar conflictos con builtins
from pyspark.sql.types import *


def generar_historial_spark(pdf_clientes, ncols):
    import pandas as pd
    import numpy as np
    import random
    import builtins
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

    cols_orden = [
        "id_cliente", "fecha", "mes_simulacion", "plazo_remanente",
        "producto", "saldo", "pago", "mora", "estatus", "score", "num_renovacion"
    ]

    tipos = {
        "id_cliente": "int64", "fecha": "str", "mes_simulacion": "int32",
        "plazo_remanente": "int32", "producto": "str", "saldo": "float64",
        "pago": "float64", "mora": "int32", "estatus": "str", "score": "int32",
        "num_renovacion": "int32"
    }

    # Definimos qué columnas y tipos devolver según el parámetro ncols
    cols_seleccionadas = cols_orden[:ncols]
    tipos_seleccionados = {k: tipos[k] for k in cols_seleccionadas}

    for _, cliente in pdf_clientes.iterrows():
        # Estas variables vienen del DataFrame original (SCO_INI es float64)
        id_act = cliente['id_cliente']
        ingreso = cliente['ingreso_mensual']
        score_evolutivo = cliente['SCO_INI']
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
            contador_renovaciones = 0

            for m in range(1, 49):
                if m < mes_inicio_cliente: continue

                fecha_mov = (fecha_base + relativedelta(months=m)).strftime("%Y-%m-%d")

                prob_mora = 0.03 if score_evolutivo > 720 else 0.12 if score_evolutivo > 580 else 0.45
                dias_mora_mes = 0
                if random.random() < prob_mora:
                    dias_mora_mes = random.choice([5, 15, 30, 45, 60, 90])
                    mora_acumulada = builtins.max(mora_acumulada, dias_mora_mes)
                    score_evolutivo -= (dias_mora_mes / 3)
                else:
                    mora_acumulada = builtins.max(0, mora_acumulada - 30)
                    score_evolutivo += random.randint(1, 4)

                score_evolutivo = builtins.max(300.0, builtins.min(850.0, float(score_evolutivo)))
                estatus = "Vigente" if mora_acumulada < 90 else "Cobranza"

                interes_mes = saldo * (tasa_ms if estatus == "Vigente" else 0.05)

                if estatus == "Vigente":
                    if cfg['tipo'] == "revolvente":
                        meses_remanentes = plazo_fijo if random.random() < 0.7 else builtins.max(0, meses_remanentes - 1)
                        gasto_tarjeta = monto_u * random.uniform(0.05, 0.3)
                        pago_efectuado = (gasto_tarjeta + interes_mes) if score_evolutivo > 690 and dias_mora_mes == 0 else (gasto_tarjeta + interes_mes) * random.uniform(0.1, 0.4)
                        if dias_mora_mes >= 30: pago_efectuado = 0
                        saldo = builtins.max(0.0, (saldo + gasto_tarjeta + interes_mes) - pago_efectuado)
                    else:
                        pago_efectuado = pago_base if dias_mora_mes < 30 else 0
                        saldo = builtins.max(0.0, (saldo + interes_mes) - pago_efectuado)
                        meses_remanentes = builtins.max(0, meses_remanentes - 1)

                        if (meses_remanentes == 0 or random.random() < 0.05) and saldo < (monto_u * 0.3):
                            saldo += (monto_u * 0.8)
                            meses_remanentes = plazo_fijo
                            contador_renovaciones += 1
                            estatus = "Renovado"
                else:
                    pago_efectuado = saldo * 0.02 if random.random() < 0.1 else 0
                    saldo = saldo + interes_mes - pago_efectuado
                    meses_remanentes = builtins.max(0, meses_remanentes - 1)

                # Generamos el diccionario completo, el filtrado se hace al retornar
                historial_list.append({
                    "id_cliente": int(id_act),
                    "fecha": str(fecha_mov),
                    "mes_simulacion": int(m),
                    "plazo_remanente": int(meses_remanentes),
                    "producto": str(prod),
                    "saldo": float(builtins.round(saldo, 2)),
                    "pago": float(builtins.round(pago_efectuado, 2)),
                    "mora": int(dias_mora_mes),
                    "estatus": str(estatus),
                    "score": int(builtins.round(float(score_evolutivo))),
                    "num_renovacion": int(contador_renovaciones)
                })
                if saldo <= 0 and cfg['tipo'] == "fijo" and estatus != "Renovado": break

    # --- SALIDA DE LA FUNCIÓN (Fuera del bucle de clientes) ---
    df_final = pd.DataFrame(historial_list)

    if df_final.empty:
        return pd.DataFrame(columns=cols_seleccionadas).astype(tipos_seleccionados)

    # Devolvemos solo las primeras 'ncols' columnas
    return df_final[cols_seleccionadas].astype(tipos_seleccionados)



def generar_hist_clientes_disk(pdf_clientes):

    historial_list = []
    fecha_base = datetime.strptime("2020-01-01", "%Y-%m-%d")

    productos_cfg = {
        "Nomina": {"tasa_anual": 0.22, "plazo": 24, "lim_ing": 1.5, "tipo": "fijo"},
        "Personal": {"tasa_anual": 0.38, "plazo": 36, "lim_ing": 2.2, "tipo": "fijo"},
        "TDC_1": {"tasa_anual": 0.55, "plazo": 12, "lim_ing": 0.9, "tipo": "revolvente"},
        "TDC_2": {"tasa_anual": 0.75, "plazo": 12, "lim_ing": 1.4, "tipo": "revolvente"}
    }

    # 2. Bucle de procesamiento
    for _, cliente in pdf_clientes.iterrows():
        id_act = cliente['id_cliente']
        ingreso = cliente['ingreso_mensual']
        # Usamos SCO_INI como indicaste
        score_evolutivo = float(cliente['SCO_INI'])
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

                # Lógica de Mora y Score
                prob_mora = 0.03 if score_evolutivo > 720 else 0.12 if score_evolutivo > 580 else 0.45
                dias_mora_mes = 0
                if random.random() < prob_mora:
                    dias_mora_mes = random.choice([5, 15, 30, 45, 60, 90])
                    mora_acumulada = max(mora_acumulada, dias_mora_mes)
                    score_evolutivo -= (dias_mora_mes / 3)  # Usamos / para asegurar float y redondear al final
                else:
                    mora_acumulada = max(0, mora_acumulada - 30)
                    score_evolutivo += random.randint(1, 4)

                score_evolutivo = max(300.0, min(850.0, float(score_evolutivo)))
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
                        saldo = max(0.0, (saldo + gasto_tarjeta + interes_mes) - pago_efectuado)
                    else:
                        pago_efectuado = pago_base if dias_mora_mes < 30 else 0
                        saldo = max(0.0, (saldo + interes_mes) - pago_efectuado)
                        meses_remanentes = max(0, meses_remanentes - 1)
                        if (meses_remanentes == 0 or random.random() < 0.05) and saldo < (monto_u * 0.3):
                            saldo += (monto_u * 0.8)
                            meses_remanentes = plazo_fijo
                            estatus = "Renovado"
                else:
                    pago_efectuado = saldo * 0.02 if random.random() < 0.1 else 0
                    saldo = saldo + interes_mes - pago_efectuado
                    meses_remanentes = max(0, meses_remanentes - 1)

                historial_list.append({
                    "id_cliente": id_act,
                    "fecha": str(fecha_mov),
                    "mes_simulacion": int(m),
                    "plazo_remanente": int(meses_remanentes),
                    "producto": str(prod),
                    "saldo": float(saldo),
                    "pago": float(pago_efectuado),
                    "mora": int(dias_mora_mes),
                    "estatus": str(estatus),
                    "score": int(builtins.round(float(score_evolutivo)))
                })

                if saldo <= 0 and cfg['tipo'] == "fijo" and estatus != "Renovado": break

    # 3. Transformación y Limpieza de tipos (Evita error de truncamiento de PyArrow)
    if not historial_list:
        return pd.DataFrame(columns=['id_cliente', 'fecha', 'mes_simulacion', 'plazo_remanente',
                                     'producto', 'saldo', 'pago', 'mora', 'estatus', 'score'])

    df_temp = pd.DataFrame(historial_list)

    # Casting forzado para cumplir con el StructType de Spark
    df_temp['id_cliente'] = df_temp['id_cliente'].astype(np.int64)
    df_temp['mes_simulacion'] = df_temp['mes_simulacion'].astype(np.int32)
    df_temp['plazo_remanente'] = df_temp['plazo_remanente'].astype(np.int32)
    df_temp['mora'] = df_temp['mora'].astype(np.int32)
    df_temp['score'] = df_temp['score'].astype(np.int32)
    df_temp['saldo'] = df_temp['saldo'].astype(np.float64)
    df_temp['pago'] = df_temp['pago'].astype(np.float64)

    # 4. Escritura en disco (Plan de choque contra OOM)
    path_acumulado = "C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/tempo/"
    os.makedirs(path_acumulado, exist_ok=True)
    nombre_archivo = os.path.join(path_acumulado, f"lote_{uuid.uuid4().hex}.parquet")
    df_temp.to_parquet(nombre_archivo, index=False)

    # Retornamos solo una fila para que el Driver no colapse
    return df_temp.head(1)

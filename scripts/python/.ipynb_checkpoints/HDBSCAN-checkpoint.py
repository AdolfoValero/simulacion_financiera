

import pandas as pd
import hdbscan
import time

# 1. Carga de datos (asumiendo que ya tienes tu CSV de 1GB)
input_path = r"C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/denue/MatrizHibrida.csv"
output_path_parquet = r"C:/Users/TheAdolf/PycharmProjects/Py11SparkProj/data/denue/Matriz_Clusterizada_Jerarquica.parquet"

print("Cargando matriz...")
df = pd.read_csv(input_path)
matriz_clustering = df.values

# =====================================================================
# CAPA 1: MICRO-NODOS (Fricción a nivel de calle / Riesgo local)
# =====================================================================
print("\nCalculando Micro-nodos (barrios y corredores)...")
clusterer_micro = hdbscan.HDBSCAN(
    min_cluster_size=15,
    min_samples=5,
    metric='euclidean',
    algorithm='boruvka_kdtree',
    core_dist_n_jobs=-1
)
micro_labels = clusterer_micro.fit_predict(matriz_clustering)
df['micro_cluster_id'] = [0 if x == -1 else x + 1 for x in micro_labels]

print(f" -> Micro-nodos encontrados: {df['micro_cluster_id'].nunique() - 1}")

# =====================================================================
# CAPA 2: MACRO-NODOS / ANCLAS (Gravedad comercial / Centros económicos)
# =====================================================================
print("\nCalculando Macro-nodos (Anclas y centros comerciales)...")
clusterer_macro = hdbscan.HDBSCAN(
    min_cluster_size=200,       # Exigimos densidad masiva
    min_samples=20,             # Cortamos los puentes de baja densidad
    metric='euclidean',
    algorithm='boruvka_kdtree',
    core_dist_n_jobs=-1
)
macro_labels = clusterer_macro.fit_predict(matriz_clustering)
df['macro_cluster_id'] = [0 if x == -1 else x + 1 for x in macro_labels]

print(f" -> Macro-nodos (Anclas) encontrados: {df['macro_cluster_id'].nunique() - 1}")

# =====================================================================
# GUARDADO
# =====================================================================
print("\nGuardando resultados consolidados...")
df.to_parquet(output_path_parquet, index=False)
# df.to_csv("ruta.csv", index=False) # Descomenta si también quieres el CSV

print("¡Proceso completado! Tienes ambas resoluciones en la misma base.")






library(arrow)
library(dplyr)

# 1. Definir rutas y el vector de entidades ("01" al "32")
ruta_datos <- "/home/rstudio/data/processed/LocRur/"
ruta_delta_vivs <- "/home/rstudio/data/processed/DeltaLake/Viviendas_Nacional/"
ruta_delta_per <- "/home/rstudio/data/processed/DeltaLake/Personas_Nacional/"

ENTIDADES <- sprintf("%02d", 1:32) 

# =================================================================
# 2. CICLO FOR: VIVIENDAS (Arrow Native)
# =================================================================
for (ent in ENTIDADES) {
  archivo_viv <- paste0(ruta_datos, ent, "_Vivs.parquet") 
  
  if (file.exists(archivo_viv)) {
    df_viv <- read_parquet(archivo_viv) %>%
      filter(ID_VPH > 0)
    
    # Arrow (C++) escribe directamente las particiones físicas en el disco 
    # Anexando los datos sin saturar tu memoria RAM
    write_dataset(
      dataset = df_viv,
      path = ruta_lake_vivs,
      format = "parquet",
      partitioning = "CVE_ENT",
      # Super importante: Aseguramos que cada estado tenga un nombre de archivo único
      # dentro de su carpeta para evitar que Arrow sobreescriba accidentalmente
      basename_template = paste0("lote_", ent, "_{i}.parquet") 
    )
    cat("Viviendas: Entidad", ent, "procesada y particionada.\n")
  }
}

# =================================================================
# 3. CICLO FOR: PERSONAS (Arrow Native)
# =================================================================
for (ent in ENTIDADES) {
  archivo_per <- paste0(ruta_datos, ent, "_POB.parquet") 
  
  if (file.exists(archivo_per)) {
    df_per <- read_parquet(archivo_per)
    
    write_dataset(
      dataset = df_per,
      path = ruta_lake_per,
      format = "parquet",
      partitioning = "CVE_ENT",
      basename_template = paste0("lote_", ent, "_{i}.parquet")
    )
    cat("Personas: Entidad", ent, "procesada y particionada.\n")
  }
}

print("¡Archivos Parquet particionados exitosamente con Arrow!")


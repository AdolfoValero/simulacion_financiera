library(sf)
library(arrow)
library(dplyr)
library(tidyr) 
library(furrr)
library(stringr)


# Asumiendo que manzanas_maestro ya pasó por su limpieza de NAs
ent <- "01"

ENTIDADES <- 
  c( "01", "02", "03", "04", "05", "06", "07", "08", "09",
     "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
     "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
     "30", "31", "32")
ent <- "02"
for (ent in ENTIDADES ){
    
  file_gpkg <- paste0("/home/rstudio/data/processed/LocRur/", ent, "_LocRur.gpkg")
  
  # Leemos el GeoPackage directamente
  manzanas <- st_read(file_gpkg, stringsAsFactors = FALSE)
  manzanas <- manzanas%>% st_make_valid() 
  manzanas <- manzanas %>%  mutate(
      VIVTOT = coalesce(VIVTOT, 5),
      TVIVPAR = coalesce(TVIVPAR, 3),    
      VIVPAR_HAB = coalesce(VIVPAR_HAB, 3),
      POBTOT = coalesce(POBTOT, 11),
      TOTHOG = coalesce(TOTHOG, 3)
    )
  
  manzanas  <- manzanas%>% filter(VIVTOT > 0)
  
  plan(multisession, workers = parallel::detectCores() - 1)
  on.exit(plan(sequential))
  listaGEO   <- as.list(manzanas$CVEGEO)
  
  lista_resultados <- 
    future_map(listaGEO, function(mun) {
      manzanas_mun <- manzanas %>% filter(CVEGEO == mun)
      puntos_viviendas <- st_sample(manzanas_mun, size = manzanas_mun$VIVTOT, type = "regular", exact = TRUE )
      ViviendasxMZA <- st_sf(geometry = puntos_viviendas) %>%
      st_join(manzanas_mun %>% 
        select(CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, AMBITO, POBTOT, VIVTOT, TVIVHAB, TVIVPAR, VIVPAR_HAB, TOTHOG
  #             , POBFEM, POBMAS, VIVPARH_CV, TVIVPARHAB, VIVPAR_DES, VIVPAR_UT
        ) ) %>% mutate( 
          ID_DOMICILIO = str_pad(row_number(), width = 6, side = "left", pad = "0")) # Generamos la llave primaria física
    return(ViviendasxMZA)
  })
  plan(sequential)
  # 3. Al final, unimos todo
  viviendas <- bind_rows(lista_resultados)
  
  file1  <- paste("/home/rstudio/data/processed/LocRur/", ent, "_Viv.gpkg", sep="")
  file2  <- paste("/home/rstudio/data/processed/LocRur/", ent, "_Vivs.parquet", sep="")
  
  st_write(viviendas, file1, delete_dsn = TRUE)
#  st_write(viviendas, file1, append = FALSE, delete_layer = TRUE)  
  sfarrow::st_write_parquet(viviendas, file2)
  rm(lista_resultados);

  # =========================================================
  # MARCO 2: HOGARES (La Capa Económica)
  # =========================================================
  # Supuesto base: 1 Domicilio = 1 Hogar principal.
  # El reto: Repartir la POBTOT exacta de la manzana entre estos hogares.
  
  marco_hogares <- viviendas%>%filter(POBTOT>0) %>%
    st_drop_geometry() %>% # Soltamos la geometría para acelerar el procesamiento de tablas
    group_by(CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, AMBITO) %>%
    mutate(
      # Matemática para cuadrar habitantes:
      # 1. Asignamos el promedio base truncado a todos los hogares
      INTEGRANTES_BASE = floor(POBTOT / VIVPAR_HAB ),
      # 2. Calculamos el residuo de personas que sobran por la división
      RESIDUO_PERSONAS = POBTOT %% VIVPAR_HAB ,
      # 3. Repartimos 1 persona extra a los primeros 'N' hogares hasta agotar el residuo
      RECIBE_EXTRA = row_number() <= RESIDUO_PERSONAS,
      # 4. Totalizamos
      TOTAL_INTEGRANTES = INTEGRANTES_BASE + ifelse(RECIBE_EXTRA, 1, 0),
      # Generamos la llave primaria del hogar
      ID_HOGAR = str_pad(row_number(), width = 3, side = "left", pad = "0")
    ) %>% ungroup() %>% select(CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, AMBITO, 
                               ID_HOGAR, ID_DOMICILIO, TOTAL_INTEGRANTES)
  
  file1  <- paste0("/home/rstudio/data/processed/LocRur/", ent, "_HOG.parquet")
  write_parquet(marco_hogares, file1)
  
  
  
  # =========================================================
  # MARCO 3: CLIENTES POTENCIALES (La Capa Demográfica)
  # =========================================================
  marco_clientes <- marco_hogares %>%
    # La magia de tidyr: si un hogar tiene 4 integrantes, clona la fila 4 veces
    uncount(TOTAL_INTEGRANTES) %>%
    group_by(ID_HOGAR) %>%
    mutate(
      NUMERO_EN_HOGAR =row_number(), 
      ID_PERSONA = str_pad(row_number(), width = 3, side = "left", pad = "0"),
      # Inyectamos una semilla de roles lógicos
      ROL_HOGAR = case_when(
        NUMERO_EN_HOGAR == 1 ~ "JEFE_FAMILIA",
        NUMERO_EN_HOGAR == 2 ~ "CONYUGE",
        TRUE ~ "DEPENDIENTE"
      )
    ) %>%
    ungroup() %>%
    select(CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, AMBITO, 
           ID_PERSONA, ID_HOGAR, ROL_HOGAR)  
  
  file1  <- paste0("/home/rstudio/data/processed/LocRur/", ent, "_POB.parquet")
  write_parquet(marco_hogares, file1)

}



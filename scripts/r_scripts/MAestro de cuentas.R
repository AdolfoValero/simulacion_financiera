library(sf)
library(arrow)
library(dplyr)
library(tidyr) 
library(furrr)
library(stringr)
library(h3jsr) # Librería nativa para indexación H3 en R


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
      TOTHOG = (VIVPAR_HAB>0)*coalesce(TOTHOG, 3)
    )
  
  manzanas  <- manzanas%>% filter(VIVTOT > 0)
  
  plan(multisession, workers = parallel::detectCores() - 1)
  on.exit(plan(sequential))
  mun <- "0100100010229008"
  listaGEO   <- as.list(manzanas$CVEGEO)
  
  lista_resultados <- 
    future_map(listaGEO, function(mun) {
      manzanas_mun <- manzanas %>% filter(CVEGEO == mun)
      ViviendasxMZA <- st_sample(manzanas_mun, size =  ceiling(1.5 * manzanas_mun$VIVTOT), type = "regular", exact = TRUE )%>%
        st_sf()
      if(dim(ViviendasxMZA)[1]<manzanas_mun$VIVTOT[1]){
        ViviendasxMZA <- st_sample(manzanas_mun, size =  ceiling(1.5 * manzanas_mun$VIVTOT), type = "random", exact = TRUE )%>%
          st_sf()
      }
      ViviendasxMZA <-  ViviendasxMZA %>% st_join(manzanas_mun %>% 
        select(CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, AMBITO, POBTOT, VIVTOT, TVIVHAB, TVIVPAR, VIVPAR_HAB, TOTHOG
  #             , POBFEM, POBMAS, VIVPARH_CV, TVIVPARHAB, VIVPAR_DES, VIVPAR_UT
        ) ) %>% mutate( ID_DOMICILIO = row_number()) # Generamos la llave primaria física
      
      ViviendasxMZA  <- ViviendasxMZA%>%filter(ID_DOMICILIO %in% sample(ID_DOMICILIO, unique(manzanas_mun$VIVTOT)))
      
      ViviendasxMZA  <- 
        ViviendasxMZA%>%mutate(ID_DOMICILIO = str_pad(row_number(), width = 6, side = "left", pad = "0"))%>%
          mutate(ID_VPH = (row_number() <= VIVPAR_HAB )*1, HOGxVPH = 1)%>%
          mutate(HOGxVPH = HOGxVPH*ID_VPH, ID_VPH = row_number()*ID_VPH)%>%
          mutate(HOGxVPH = HOGxVPH + 1*(row_number()<=(TOTHOG - VIVPAR_HAB)))
      
      ViviendasxMZA  <- ViviendasxMZA%>%mutate(
        PERxVIV = floor(POBTOT*(HOGxVPH/sum(HOGxVPH))),
        PERxVIV = PERxVIV+ 1*(row_number() <= (POBTOT - sum(PERxVIV)))
      )
      
      ViviendasxMZA <- 
         ViviendasxMZA %>% mutate(PERxVIV = ifelse(is.nan(PERxVIV), POBTOT/VIVTOT, PERxVIV)) %>% 
         mutate(PERxVIV = ifelse(is.nan(PERxVIV), 0, PERxVIV)) %>% 
         select( CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, AMBITO, ID_DOMICILIO, ID_VPH, 
                 HOGxVPH, PERxVIV)
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
  
  file_gpkg  <- paste("/home/rstudio/data/processed/LocRur/", ent, "_Viv.gpkg", sep="")
  viviendas  <- st_read(file_gpkg, stringsAsFactors = FALSE)%>% st_make_valid() 
  viviendas  <- viviendas%>% filter(ID_VPH > 0 )

  marco_hogares <- viviendas %>% st_drop_geometry() %>% uncount(HOGxVPH )%>%
       group_by( CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, AMBITO, ID_DOMICILIO, ID_VPH, PERxVIV)%>% 
       mutate(ID_HOGAR = str_pad(row_number() , width = 3, side = "left", pad = "0"), TOTHOG = n()) %>% 
    mutate(PERxHOG = floor(PERxVIV/TOTHOG), PERxHOG = PERxHOG + 1*(row_number() <= (PERxVIV - sum(PERxHOG)))           
           ) %>% ungroup()
  marco_hogares$TOTHOG  <- NULL
  marco_hogares$PERxVIV <- NULL 
  file1  <- paste0("/home/rstudio/data/processed/LocRur/", ent, "_HOG.parquet")
  write_parquet(marco_hogares, file1)
  
  
  
  # =========================================================
  # MARCO 3: CLIENTES POTENCIALES (La Capa Demográfica)
  # =========================================================
  marco_clientes <- marco_hogares %>% uncount(PERxHOG) %>%
    group_by( CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, AMBITO, ID_DOMICILIO, ID_VPH, ID_HOGAR )%>% 
    mutate(ID_PER = str_pad(row_number() , width = 3, side = "left", pad = "0"))
  marco_hogares <- marco_clientes %>% ungroup()
  
  file1  <- paste0("/home/rstudio/data/processed/LocRur/", ent, "_POB.parquet")
  write_parquet(marco_clientes, file1)

}





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
  file_gpkg  <- paste0("/home/rstudio/data/processed/LocRur/", ent, "_Viv.gpkg")
  viviendas  <- st_read(file_gpkg, stringsAsFactors = FALSE)%>% st_make_valid() %>% st_transform(crs = 4326)
  
  coords <- st_coordinates(viviendas)
  viviendas$Longitud <- coords[, "X"]
  viviendas$Latitud  <- coords[, "Y"]
  
  file2  <- paste0("/home/rstudio/data/processed/LocRur/", ent, "_Vivs.parquet")
  sfarrow::st_write_parquet(viviendas, file2)
  rm(lista_resultados);
}



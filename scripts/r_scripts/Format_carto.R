library(sf)
library(dplyr)
library(data.table)
ENTIDADES <- 
  c( "01", "02", "03", "04", "05", "06", "07", "08", "09",
     "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
     "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
     "30", "31", "32")
ent <- "02"
for (ent in ENTIDADES){
  # 1. Carga y validación de capas base del INEGI
  file1  <- paste("/home/rstudio/data/raw/MGEN/",ent,"/CD/", ent, "ar.shp", sep="")
  file2  <- paste("/home/rstudio/data/raw/MGEN/",ent,"/CD/", ent, "lpr.shp", sep="")
  
  ageb_rural <- st_read(file1, stringsAsFactors = FALSE) %>% st_make_valid()
  loc_rurales <- st_read(file2, stringsAsFactors = FALSE) %>% st_make_valid()
  
  if(st_crs(ageb_rural) != st_crs(loc_rurales)) {
    loc_rurales <- st_transform(loc_rurales, st_crs(ageb_rural))
  }
  
  # Inicializar lista contenedora y extraer identificadores de municipios
  burbujas_lista <- list()
  municipios <- unique(ageb_rural$CVE_MUN)
  
  # 2. Procesamiento cartográfico iterativo por municipio
  for (mun in municipios) {
    
    # Filtrar la cobertura correspondiente al municipio actual
    ageb_mun <- ageb_rural %>% filter(CVE_MUN == mun)
    loc_mun <- loc_rurales %>% filter(CVE_MUN == mun)
    
    # Control de seguridad si el municipio no cuenta con localidades rurales
    if (nrow(loc_mun) == 0) next
    
    # Generar polígonos de Voronoi usando la extensión del municipio
    caja_envolvente <- st_as_sfc(st_bbox(ageb_mun))
    puntos_unidos <- st_union(loc_mun)
    voronoi_geoms <- st_voronoi(puntos_unidos, envelope = caja_envolvente)
    voronoi_poligonos <- st_collection_extract(voronoi_geoms, "POLYGON")
    voronoi_sf <- st_sf(geometry = voronoi_poligonos, crs = st_crs(ageb_mun))
    
    # Transferir atributos de los puntos semilla a los polígonos de Voronoi
    voronoi_con_datos <- st_join(voronoi_sf, loc_mun, join = st_intersects)
    
    # Recortar extensión de Voronoi con el molde de las AGEB rurales municipales
    burbujas_mun_final <- st_intersection(voronoi_con_datos, ageb_mun)
    
    # Crear máscara de amortiguamiento de 3km para contener la dispersión rural
    buffers_individuales <- st_buffer(loc_mun, dist = 3000)
    mascara_3km <- st_union(buffers_individuales)
    
    # Aplicar el recorte de distancia máxima de 3km a las burbujas del municipio
    burbujas_mun_3km <- st_intersection(burbujas_mun_final, mascara_3km)
    
    # Disolver fronteras internas y consolidar la estructura del objeto municipal
    burbujas_mun_agg <- burbujas_mun_3km %>%
      group_by(CVEGEO, CVE_ENT, CVE_MUN, CVE_LOC, CVE_AGEB, CVE_MZA, NOMGEO, PLANO) %>%
      summarise(geometry = st_union(geometry), .groups = "drop")
    
    # Almacenar el objeto sf resultante en la lista indexada
    burbujas_lista[[mun]] <- cburbujas_mun_agg <- burbujas_mun_agg
  }
  
  # 3. Consolidación de la pedacería municipal en un único objeto maestro sf
  burbujas_rurales_3km <- bind_rows(burbujas_lista)
  
  # 4. Almacenamiento de los resultados consolidados en disco
  
  
  Mfile  <- paste("/home/rstudio/data/raw/MGEN/",ent,"/CD/", ent, "m.shp", sep="")
  ageb_urban <- st_read(Mfile, stringsAsFactors = FALSE) %>% st_make_valid()
  
  if(st_crs(ageb_urban) != st_crs(burbujas_rurales_3km)) {
    burbujas_rurales_3km <- st_transform(burbujas_rurales_3km, st_crs(ageb_urban))
  }  
  # Unir dos capas
  burbujas_rurales_3km <- bind_rows(ageb_urban, burbujas_rurales_3km)
  
  burbujas_rurales_3km <- burbujas_rurales_3km %>% st_make_valid()
  
  
  
  Pob    <- as.data.table(read.csv(paste("/home/rstudio/data/raw/CPV2020/RESAGEBURB_", ent, "CSV20.csv", sep="")))
  Pabs   <- Pob[MZA == 0 ]
  Pob    <- Pob[MZA != 0 ]
  
  variabs  <-  colnames(Pob)[-c(1:8)]
  vari     <- variabs[1]
  
  for (vari in variabs){
    Pob[, (vari) := as.integer(get(vari))]
  }
  
  # %03d significa: rellena con ceros (0) hasta llegar a 3 dígitos de un número entero (d)
  library(dplyr)
  library(stringr)
  
  # Esto rellenará de ceros a la izquierda sin importar si hay letras
  Pob <- Pob %>%
    mutate(
      # Aseguramos que sea texto primero (por si acaso R lo leyó raro)
      ENTIDAD = as.character(ENTIDAD         ),
      CVE_ENT = str_pad(ENTIDAD         , width = 2, side = "left", pad = "0"),

      MUN = as.character(MUN),
      CVE_MUN = str_pad(MUN, width = 3, side = "left", pad = "0"),
      
      LOC = as.character(LOC),
      CVE_LOC = str_pad(LOC, width = 4, side = "left", pad = "0"),
      
      AGEB = as.character(AGEB),
      CVE_AGEB = str_pad(AGEB, width = 4, side = "left", pad = "0"),

      MZA = as.character(MZA),
      CVE_MZA = str_pad(MZA, width = 3, side = "left", pad = "0")
      
    )
  variabs <- unique(c("CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA", 
                      "VIVTOT", "TVIVHAB", "TVIVPAR", "VIVPAR_HAB", "POBTOT", "POBFEM", "POBMAS",
                      "POCUPADA", "POCUPADA_F", "POCUPADA_M", "PDESOCUP", "PDESOCUP_F", "PDESOCUP_M",
                      variabs))


  Pob   <- Pob[, variabs, with = F]

  burbujas_con_censo <- burbujas_rurales_3km %>%
    left_join( Pob,  by = c("CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA")
    )
  
#  Pabs[MUN == 0][,.(POBTOT, POBFEM, POBMAS, VIVTOT, TVIVHAB, TVIVPAR, VIVPAR_HAB, VIVPARH_CV, TVIVPARHAB, VIVPAR_DES, VIVPAR_UT)]
#  sum(Pob$POBTOT, na.rm=T);  sum(burbujas_con_censo$POBTOT, na.rm=T)
#  sum(Pob$VIVPAR_HAB, na.rm=T);   sum(burbujas_con_censo$VIVPAR_HAB, na.rm=T)
  
  burbujas_con_censo <- burbujas_con_censo %>%
    mutate(
      # Si POBTOT es NA, pon 11; si no, deja el valor original
      POBTOT = coalesce(POBTOT, 11),
      
      # Si VIVTOT es NA, pon 3; si no, deja el valor original
      VIVPAR_HAB = coalesce(VIVPAR_HAB, 3)
    )

#  Pabs[MUN == 0][,.(POBTOT, POBFEM, POBMAS, VIVTOT, TVIVHAB, TVIVPAR, VIVPAR_HAB, VIVPARH_CV, TVIVPARHAB, VIVPAR_DES, VIVPAR_UT)]
#  sum(Pob$POBTOT, na.rm=T);  sum(burbujas_con_censo$POBTOT, na.rm=T)
#  sum(Pob$VIVPAR_HAB, na.rm=T);   sum(burbujas_con_censo$VIVPAR_HAB, na.rm=T)
  
  burbujas_con_censo <- burbujas_con_censo %>% st_make_valid()
  
  file1  <- paste("/home/rstudio/data/processed/LocRur/", ent, "_LocRur.gpkg", sep="")
  file2  <- paste("/home/rstudio/data/processed/LocRur/", ent, "_LocRur.parquet", sep="")

  st_write(burbujas_con_censo, file1, append = FALSE)
  sfarrow::st_write_parquet(burbujas_con_censo, file2)
  
}





library(arrow)
library(dplyr)
library(tidyr)
library(lubridate)
library(sf)
library(h3jsr)
library(data.table)

ruta_delta_vivs <- "/home/rstudio/data/processed/DeltaLake/Viviendas_Nacional/"
ruta_delta_per <- "/home/rstudio/data/processed/DeltaLake/Personas_Nacional/"

setwd("C:/Users/TheAdolf/DOCKER/Simulacion_financiera/")
ruta_lake_vivs <- "C:/Users/TheAdolf/DOCKER/Simulacion_financiera/data/processed/DeltaLake/Viviendas_Nacional/"
ruta_lake_per <- "C:/Users/TheAdolf/DOCKER/Simulacion_financiera/data/processed/DeltaLake/Personas_Nacional/"

ds_vivs <- open_dataset(ruta_lake_vivs)
ds_pers <- open_dataset(ruta_lake_per)






tabla_ids_nacional <- ds_vivs %>% select(CVEGEO,CVE_MZA,ID_DOMICILIO) %>% collect()
tabla_ids_nacional <- tabla_ids_nacional%>% mutate(IDVIV = paste0(CVEGEO,CVE_MZA,ID_DOMICILIO))

ME_3   <- as.data.table(read.csv("data/processed/MasasEconomicas H3-3.csv"))
ME_4   <- as.data.table(read.csv("data/processed/MasasEconomicas H3-4.csv"))
ME_5   <- as.data.table(read.csv("data/processed/MasasEconomicas H3-5.csv"))
ME_6   <- as.data.table(read.csv("data/processed/MasasEconomicas H3-6.csv"))
ME_7   <- as.data.table(read.csv("data/processed/MasasEconomicas H3-7.csv"))
ME_8   <- as.data.table(read.csv("data/processed/MasasEconomicas H3-8.csv"))

columnas_indices <- c("Pob_Flotante", "Masa_Salarial", "Indice_Zombificacion", "Vuln_Efectivo", 
                      "FAC_SOCECO", "VAL_FRIC", "Tasa_Resiliencia", "Indice_Entropia", "Mixed_Use_Score")

for (k in 3:8){
  eval(parse(text = paste0("DATAPCA <- copy(ME_", k,")")))
  datos_indices <- DATAPCA[, ..columnas_indices]
  for (col in columnas_indices) setnafill(datos_indices, fill = 0, cols = col)
  pca_global <- prcomp(scale(datos_indices))
  summary(pca_global)
  DATAPCA[, c("ECO_Desarrollo", "ECO_Dinamismo", "ECO_Estabilidad", 
           "ECO_Vulnerabilidad", "ECO_Friccion") := 
         as.data.table(pca_global$x[, 1:5])]
  eval(parse(text = paste0("ME_", k,"  <- copy(DATAPCA)")))
}

fecha_base        <- as.Date("2000-01-03") 
semana_objetivo   <- 1200

week     <- 1

for (week in 1:semana_objetivo){
    VIV_SAM<- data.table(NULL)
    FECHAS   <- fecha_base + (week - 1) * 7 + 0:4
    
    set.seed(2026)
    NN <- round(1500*1.05^(0.05*week/52))
    viviendas_seleccionadas <- tabla_ids_nacional %>% 
      slice_sample(n = rbinom(1, NN, 0.8)) %>% pull(IDVIV)
    
    VIV_SAM <- ds_vivs %>% mutate(IDVIV = paste0(CVEGEO,CVE_MZA,ID_DOMICILIO))%>%
      filter(IDVIV %in% viviendas_seleccionadas) %>%
      collect()
    
    
    VIV_SAM$CVE_AGEB  <- NULL
    VIV_SAM$CVE_MZA   <- NULL
    VIV_SAM$AMBITO    <- NULL
    VIV_SAM$ID_VPH    <- NULL
    VIV_SAM$HOGxVPH   <- NULL
    VIV_SAM$PERxVIV   <- NULL
    VIV_SAM$geom      <- NULL
    
    VIV_SAM$CVEGEO         <- NULL
    VIV_SAM$ID_DOMICILIO   <- NULL
    VIV_SAM$IDVIV          <- NULL
    
    VIV_SAM <- as.data.table(st_drop_geometry(VIV_SAM))
    VIV_SAM[, FECHA_ALTA := as.POSIXct(sample(FECHAS, .N, replace = TRUE)) + runif(.N, 9 * 3600, 16 * 3600)]
    
    
    inicio <- as.POSIXct("2026-06-18 08:00:00")
    fin    <- as.POSIXct("2026-06-18 20:00:00")
    
    
    matriz_coords <- as.matrix(VIV_SAM[, c("Longitud", "Latitud")])
    VIV_SAM$H33   <-  point_to_cell(matriz_coords, res = 3)
    VIV_SAM$H34   <-  point_to_cell(matriz_coords, res = 4)
    VIV_SAM$H35   <-  point_to_cell(matriz_coords, res = 5)
    VIV_SAM$H36   <-  point_to_cell(matriz_coords, res = 6)
    VIV_SAM$H37   <-  point_to_cell(matriz_coords, res = 7)
    VIV_SAM$H38   <-  point_to_cell(matriz_coords, res = 8)
    
    
    
    
    
    library(data.table)
    
    IndexEconom  <- c("Masa_Salarial", "FAC_SOCECO", "Pob_Flotante", "Indice_Entropia", 
                      "Indice_Zombificacion", "Tasa_Resiliencia", "Mixed_Use_Score", "Vuln_Efectivo",
                      "ECO_Desarrollo", "ECO_Dinamismo", "ECO_Estabilidad", "ECO_Vulnerabilidad",
                      "ECO_Friccion")
    ineco  <- IndexEconom[1]
    for (ineco in IndexEconom){
      
      eval(parse(text = paste0("VIV_SAM[ME_8, on = .(H38 = H3_Index), R8_IE := i.", ineco, "]")))
      eval(parse(text = paste0("VIV_SAM[ME_7, on = .(H37 = H3_Index), R7_IE := i.", ineco, "]")))
      eval(parse(text = paste0("VIV_SAM[ME_6, on = .(H36 = H3_Index), R6_IE := i.", ineco, "]")))
      eval(parse(text = paste0("VIV_SAM[ME_5, on = .(H35 = H3_Index), R5_IE := i.", ineco, "]")))
      eval(parse(text = paste0("VIV_SAM[ME_4, on = .(H34 = H3_Index), R4_IE := i.", ineco, "]")))
      eval(parse(text = paste0("VIV_SAM[ME_3, on = .(H33 = H3_Index), R3_IE := i.", ineco, "]")))
      
      eval(parse(text = paste0("VIV_SAM[, ", ineco, " := fcoalesce(R8_IE, R7_IE, R6_IE, R5_IE, R4_IE, R3_IE)]")))
    }
    
    VIV_SAM[, c("R8_IE", "R7_IE", "R6_IE", "R5_IE", "R4_IE", "R3_IE", "H33", "H34", "H35", 
                "H36", "H37", "H38") := NULL]
    
    
    
    
    
    EducacionBasica <- c("Sin Educación", "Primaria", "Secundaria")
    PlazosPosibles <- c(12, 24, 36, 48, 60)
    
    library(dplyr)
    library(lubridate)
    library(tidyr) # Para replace_na
    
    EducacionBasica <- c("Sin Educación", "Primaria", "Secundaria")
    PlazosPosibles <- c(12, 24, 36, 48, 60)
    
    # ==============================================================================
    # FASE 1: VARIABLES DEMOGRÁFICAS Y LABORALES
    # ==============================================================================
    clientes <- 
      VIV_SAM %>% 
      mutate( id_cliente = row_number(),
        # Demografía Base
        sexo = sample(c("M", "F"), n(), replace = TRUE, prob = c(0.48, 0.52)),
        edad = (as.numeric(substr(Sys.Date(),1,4)) - as.numeric(substr(fecha_base,1,4))) + 
                    round(rbeta(n(), 2.5, 3.5) * (75 - 18) + 18),
        Fecha_nacimiento = Sys.Date() - years(edad) - days(sample(1:365, n(), replace = TRUE)),
        
        estado_civil = case_when(
          edad < 25 ~ sample(c("Soltero(a)", "Casado(a)"), n(), replace = TRUE, prob = c(0.90, 0.10)),
          edad < 35 ~ sample(c("Soltero(a)", "Casado(a)", "Divorciado(a)"), n(), replace = TRUE, prob = c(0.45, 0.50, 0.05)),
          edad < 55 ~ sample(c("Soltero(a)", "Casado(a)", "Divorciado(a)", "Viudo(a)"), n(), replace = TRUE, prob = c(0.20, 0.60, 0.15, 0.05)),
          TRUE      ~ sample(c("Soltero(a)", "Casado(a)", "Divorciado(a)", "Viudo(a)"), n(), replace = TRUE, prob = c(0.10, 0.45, 0.15, 0.30))
        ),
        
        nivel_edu = sample(
          c("Sin Educación", "Primaria", "Secundaria", "Bachiller", "Universidad", "Posgrado"), 
          n(), replace = TRUE, prob = c(0.05, 0.15, 0.30, 0.25, 0.20, 0.05)
        ),
        
        numero_de_hijos = case_when(
          edad < 22 ~ 0,
          edad < 30 ~ rpois(n(), lambda = 0.5),
          edad < 40 ~ rpois(n(), lambda = 1.5),
          TRUE      ~ rpois(n(), lambda = 2.2)
        ),
        
        NUM_DEP = case_when(
          nivel_edu %in% EducacionBasica ~ floor(runif(n()) * 6),
          nivel_edu == "Bachiller"       ~ floor(runif(n()) * 5),
          nivel_edu == "Universidad"     ~ floor(runif(n()) * 4),
          TRUE                           ~ floor(runif(n()) * 2) 
        ),
        
        # Datos Sintéticos Rápidos
        empresa = paste0("Empresa_MX_", sample(1:5000, n(), replace = TRUE)),
        Empleo = paste0("Posicion_", sample(1:1000, n(), replace = TRUE)),
        Telefono = paste0("55", sample(10000000:99999999, n(), replace = TRUE)),
        email = paste0("usr_", id_cliente, "_", sample(100:999, n(), replace=TRUE), "@mail.com"),
        zip = paste0("CP_", sample(10000:99999, n(), replace = TRUE)),
        ANT_LAB_MES = floor(runif(n()) * 480)
      )
    
    # ==============================================================================
    # FASE 2: CÁLCULO DE MODIFICADORES GEOECONÓMICOS (H3)
    # ==============================================================================
    clientes <- clientes %>%
      mutate(
        # 1. ESTANDARIZACIÓN (Z-Score): Convertir valores crudos a escala de -3 a 3.
        # scale() hace que la media sea 0 y la varianza 1.
        across(c(Masa_Salarial, FAC_SOCECO, Pob_Flotante, Indice_Entropia, 
                 Indice_Zombificacion, Tasa_Resiliencia, Mixed_Use_Score, Vuln_Efectivo), 
               ~ as.numeric(scale(.))),
        
        # 2. PREVENCIÓN DE NULOS: En un Z-Score, 0 significa "Exactamente en el promedio".
        # Así que reemplazar NA por 0 es matemáticamente impecable.
        across(c(Masa_Salarial, FAC_SOCECO, Pob_Flotante, Indice_Entropia, 
                 Indice_Zombificacion, Tasa_Resiliencia, Mixed_Use_Score, Vuln_Efectivo), 
               ~ replace_na(., 0)),
        # 3. PONDERACIONES ELÁSTICAS (Ahora controladas)
        # Ejemplo: Si Masa_Salarial es altísima (Z = 2) y FAC_SOCECO también (Z = 2)
        # mod_ingreso = (2 * 0.15) + (2 * 0.05) = 0.40 -> El cliente ganará 40% más que la base.
        
        # Reduje el peso de FAC_SOCECO al 5% para mitigar la redundancia que notaste.
        mod_ingreso    = (Masa_Salarial * 0.15) + (FAC_SOCECO * 0.05), 
        mod_gasto      = (Pob_Flotante * 0.10) + (Indice_Entropia * 0.05) + (Indice_Zombificacion * 0.05),
        # Aquí el patrimonio sí descansa más en la riqueza residencial (FAC_SOCECO) y la resiliencia
        mod_patrimonio = (Tasa_Resiliencia * 0.20) + (FAC_SOCECO * 0.15),
        mod_uso_tc     = (Mixed_Use_Score * 0.25) - (Vuln_Efectivo * 0.15)
      )
    # ==============================================================================
    # FASE 3: VARIABLES FINANCIERAS Y DE BURÓ DE CRÉDITO (COLA PESADA)
    # ==============================================================================
    clientes <- clientes %>% 
      mutate( 
        # 3.1 Ingresos (Lognormal) y Gastos (Beta) + Multiplicador H3
        # rlnorm(n, meanlog, sdlog): meanlog es el logaritmo natural de la mediana deseada.
        ingreso_mensual = case_when( 
          nivel_edu %in% EducacionBasica ~ rlnorm(n(), meanlog = log(8000), sdlog = 0.3),
          nivel_edu == "Bachiller"       ~ rlnorm(n(), meanlog = log(13000), sdlog = 0.4),
          nivel_edu == "Universidad"     ~ rlnorm(n(), meanlog = log(20000), sdlog = 0.5),
          TRUE                           ~ rlnorm(n(), meanlog = log(40000), sdlog = 0.7) # Cola más pesada para posgrados
        ) * (1 + mod_ingreso), # <--- Inyección H3
        
        # El gasto ahora es estrictamente una proporción realista del ingreso (ej. 60% a 95%)
        # Usamos rbeta(n, shape1=7, shape2=2) que tiene un pico alrededor de 0.75.
        gasto_mensual = (ingreso_mensual * rbeta(n(), shape1 = 7, shape2 = 2)) * (1 + mod_gasto),
        
        capacidad_ahorro = ingreso_mensual - gasto_mensual,
        
        # 3.2 Patrimonio (Lognormal de altísima varianza) y Proyecciones
        ING_ANUAL = (ingreso_mensual * 12) + rlnorm(n(), meanlog = log(15000), sdlog = 0.8),
        GAS_ANUAL = (gasto_mensual * 12) + rlnorm(n(), meanlog = log(10000), sdlog = 0.5),
        
        PATR_EST = case_when(
          nivel_edu %in% EducacionBasica ~ rlnorm(n(), log(100000), 0.5),
          nivel_edu == "Bachiller"       ~ rlnorm(n(), log(200000), 0.7),
          nivel_edu == "Universidad"     ~ rlnorm(n(), log(400000), 0.9),
          TRUE                           ~ rlnorm(n(), log(1000000), 1.2) # Patrimonio extremo posible
        ) * (1 + mod_patrimonio), # <--- Inyección H3
        
        tiene_auto = case_when(
          nivel_edu %in% EducacionBasica ~ ifelse(runif(n()) > 0.10, "Sí", "No"),
          nivel_edu == "Bachiller"       ~ ifelse(runif(n()) > 0.25, "Sí", "No"),
          nivel_edu == "Universidad"     ~ ifelse(runif(n()) > 0.40, "Sí", "No"),
          TRUE                           ~ ifelse(runif(n()) > 0.60, "Sí", "No") 
        ),
        
        # 3.3 Riesgo, Scores y Portafolio
        prob_fraude = rbeta(n(), 2, 50), # Pico en 0.03, casi nadie tiene alta prob, pero algunos sí
        
        SCO_INI = case_when(
          nivel_edu %in% EducacionBasica ~ rnorm(n(), mean = 400, sd = 50),
          nivel_edu == "Bachiller"       ~ rnorm(n(), mean = 550, sd = 60),
          nivel_edu == "Universidad"     ~ rnorm(n(), mean = 650, sd = 70),
          TRUE                           ~ rnorm(n(), mean = 750, sd = 40)
        ),
        # Acotamos el score para que no rompa la escala clásica (ej. 300 a 850)
        SCO_INI = pmin(pmax(SCO_INI, 300), 850),
        SCO_ACT = pmin(pmax(SCO_INI + rnorm(n(), mean = 10, sd = 40), 300), 850),
        
        # Poisson para recuentos lógicos de productos
        NUM_CREDACTI = case_when(
          nivel_edu %in% EducacionBasica ~ rpois(n(), 0.5),
          nivel_edu == "Bachiller"       ~ rpois(n(), 1.5),
          nivel_edu == "Universidad"     ~ rpois(n(), 3.0),
          TRUE                           ~ rpois(n(), 5.0)
        ),
        
        NUM_CUENTAS = rpois(n(), lambda = 2),
        NUM_TC = rpois(n(), lambda = 1.5),
        
        # 3.4 Buró de Crédito Realista (Gamma / Lognormal)
        SAL_TOTDEU = case_when(
          nivel_edu %in% EducacionBasica ~ rlnorm(n(), log(50000), 0.6),
          nivel_edu == "Bachiller"       ~ rlnorm(n(), log(120000), 0.8),
          nivel_edu == "Universidad"     ~ rlnorm(n(), log(250000), 1.0),
          TRUE                           ~ rlnorm(n(), log(800000), 1.2)
        ),
        
        MONTO_TOTSOL = SAL_TOTDEU * rbeta(n(), 2, 5), # Lo solicitado suele ser una fracción de su deuda total
        
        # Uso de TC con Beta (La mayoría usa poco, algunos topan la tarjeta al 100%)
        UTIL_TCBase = case_when(
          nivel_edu %in% EducacionBasica ~ rbeta(n(), 1, 5), # Mayormente bajo
          nivel_edu == "Bachiller"       ~ rbeta(n(), 2, 4),
          nivel_edu == "Universidad"     ~ rbeta(n(), 3, 2), # Mayormente alto
          TRUE                           ~ rbeta(n(), 5, 2)
        ),
        UTIL_TC = pmax(0, UTIL_TCBase * (1 + mod_uso_tc)), 
        
        prob_caer_en_mora = 0.20 + (Vuln_Efectivo * 0.15),
        # Días de mora (Exponencial: muchos con pocos días, pocos con muchos días)
        MAXDIAS_MORAHIST = ifelse(runif(n()) < prob_caer_en_mora, round(rexp(n(), rate = 1/30)), 0),
        MAXDIAS_MORAHIST = pmin(MAXDIAS_MORAHIST, 360), # Topeamos en 360 días para realismo
        
        plazo_meses = sample(PlazosPosibles, n(), replace = TRUE),
        TASA_INTASIG = rbeta(n(), 2, 8) + 0.10, # Tasa base del 10% + variación Beta
        
        # ==============================================================================
        # FASE 4: REDONDEO Y LIMPIEZA FINAL
        # ==============================================================================
        across(c(ingreso_mensual, gasto_mensual, capacidad_ahorro, 
                 ING_ANUAL, GAS_ANUAL, PATR_EST, SAL_TOTDEU, MONTO_TOTSOL), ~round(., 2)),
        across(c(SCO_INI, SCO_ACT), ~round(., 0)),
        UTIL_TC = round(UTIL_TC, 4),
        TASA_INTASIG = round(TASA_INTASIG, 4)
        
      ) %>% 
      select(-starts_with("mod_"), -UTIL_TCBase, -prob_caer_en_mora, -any_of("geom"), 
             -Pob_Flotante, -Indice_Entropia, -Indice_Zombificacion, -Tasa_Resiliencia,
             -Mixed_Use_Score, -Vuln_Efectivo, -Masa_Salarial, -FAC_SOCECO )
    
    # 2.A Generar los productos (Ej: de 1 a 3 créditos por cliente)
    productos <- clientes %>%
      select(id_cliente, ingreso_mensual, SCO_ACT, FECHA_ALTA) %>% # <--- Reemplazo aquí
      mutate(num_productos = sample(1:3, n(), replace = TRUE)) %>%
      uncount(num_productos, .id = "id_producto") %>%
      mutate(
        tipo_producto = sample(c("Tarjeta", "Personal", "Automotriz"), n(), replace = TRUE),
        monto_credito = round(ingreso_mensual * runif(n(), 1.5, 5), 2),
        plazo_total = sample(c(12, 24, 36), n(), replace = TRUE),
        pago_fijo = round((monto_credito * 1.15) / plazo_total, 2), # 15% interés simple
        # Fecha de apertura aleatoria en los últimos 2 años
        # Reemplazo dentro del mutate de productos:
        fecha_apertura = as.POSIXct(FECHA_ALTA) + 
          runif(n(), 0, as.numeric(difftime(Sys.time(), as.POSIXct(FECHA_ALTA), units = "secs"))),
        # Probabilidad de mora en base al score actual
        prob_mora = case_when(SCO_ACT < 600 ~ 0.15, SCO_ACT < 700 ~ 0.08, TRUE ~ 0.02) # <--- Reemplazo aquí
      )
    
    # 2.B Expandir los créditos a su ciclo de vida mensual y calcular historial
    
    library(data.table)
    library(lubridate)
    library(dplyr)
    library(tidyr)
    
    mes_actual <- as.numeric(format(Sys.Date(), "%m"))
    anio_actual <- as.numeric(format(Sys.Date(), "%Y"))
    
    system.time({
      setDT(productos)
      productos[, dia_corte := sample(1:28, .N, replace = TRUE)]
      historial <- productos %>% uncount(plazo_total, .id = "mes_vida", .remove = FALSE)
      setDT(historial) 
      
      historial[, `:=`( meses_atras = plazo_total - mes_vida, plazo_remanente = plazo_total - mes_vida,
        mora = rbinom(.N, 1, prob = prob_mora) )]
      historial[, `:=`( mes_absoluto = mes_actual - meses_atras, pago_realizado = fcase(mora == 0, pago_fijo, default = 0),
        cambio_score = fcase(mora == 1, -15, default = 2) )]
      historial[, `:=`( anio_corte = anio_actual + floor((mes_absoluto - 1) / 12), mes_corte = ((mes_absoluto - 1) %% 12) + 1 )]
      historial[, fecha_corte := make_date(anio_corte, mes_corte, dia_corte)]
      
      setorder(historial, id_cliente, id_producto, mes_vida)
    
      historial[, `:=`( monto_pagado_acum = cumsum(pago_realizado), score_evolutivo = SCO_ACT + cumsum(cambio_score)
                        ), by = .(id_cliente, id_producto)]
    
      historial[, saldo := pmax(monto_credito - monto_pagado_acum, 0)]
      historial[, estatus := fcase( saldo <= 0, "Pagado", mora == 1, "En Atraso", default = "Al Corriente" )]
      
      setorder(historial, id_cliente, id_producto, -mes_vida)
      historial[, c("FECHA_ALTA", "meses_atras", "mes_absoluto", "anio_corte", "mes_corte", "dia_corte",
                    "monto_pagado_acum", "cambio_score", "SCO_ACT ") := NULL]
    })
    
    historial <- historial %>% mutate(anio_mes = format(fecha_corte, "%Y_%m"))
    
    write_dataset( dataset = clientes, path = "C:/Users/TheAdolf/DOCKER/Simulacion_financiera/data/processed/DataLake/Clientes/", 
                   format = "parquet" )
    write_dataset( dataset = historial, path = "C:/Users/TheAdolf/DOCKER/Simulacion_financiera/data/processed/DataLake/Historial/", 
                   format = "parquet", partitioning = "anio_mes" )
}

rm(tabla_ids_nacional)






    


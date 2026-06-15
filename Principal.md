# Documentación de mi Proyecto


- [<span class="toc-section-number">1</span>
  simulacion_financiera](#simulacion_financiera)

# simulacion_financiera

Creación de una cartera de clientes usando PySpark para entrenamiento de
modelos economicos e implementación de data-streaming y MLOps.

``` r
library(arrow)
```


    Adjuntando el paquete: 'arrow'

    The following object is masked from 'package:utils':

        timestamp

``` r
library(dplyr)
```


    Adjuntando el paquete: 'dplyr'

    The following objects are masked from 'package:stats':

        filter, lag

    The following objects are masked from 'package:base':

        intersect, setdiff, setequal, union

``` r
library(sf)
```

    Linking to GEOS 3.13.1, GDAL 3.11.4, PROJ 9.7.0; sf_use_s2() is TRUE

``` r
library(h3jsr)
```

    Warning: package 'h3jsr' was built under R version 4.5.3

``` r
library(ggplot2)

# 1. Configuración de variables
file_path <- "C:/Users/TheAdolf/DOCKER/Simulacion_financiera/data/processed/DENUExCENECO.parquet"

Ylims <- c(  21.6,   22.5)
Xlims <- c(-103, -101.7)
bbox_ags <- c(xmin = Xlims[1], ymin = Ylims[1], xmax = Xlims[2], ymax = Ylims[2])

# 2. Lectura eficiente y filtrado espacial inmediato
df <- read_parquet(file_path) %>%
  filter(longitud >= bbox_ags["xmin"], longitud <= bbox_ags["xmax"],
         latitud >= bbox_ags["ymin"], latitud <= bbox_ags["ymax"])

file_path <- "C:/Users/TheAdolf/DOCKER/Simulacion_financiera/data/raw/MGEN/mg_2025_integrado/CD/00ent.shp"

bbox_ags <- st_as_sfc(st_bbox(bbox_ags, crs = 4326))
mexico <- st_read(file_path) %>% st_transform(4326)
```

    Reading layer `00ent' from data source 
      `C:\Users\TheAdolf\DOCKER\Simulacion_financiera\data\raw\MGEN\mg_2025_integrado\CD\00ent.shp' 
      using driver `ESRI Shapefile'
    Simple feature collection with 32 features and 3 fields
    Geometry type: MULTIPOLYGON
    Dimension:     XY
    Bounding box:  xmin: 911292 ymin: 319149.1 xmax: 4083063 ymax: 2349615
    Projected CRS: MEXICO_ITRF_2008_LCC

``` r
mexico_ags <- st_crop(mexico, bbox_ags)
```

    Warning: attribute variables are assumed to be spatially constant throughout
    all geometries

``` r
# 3. Función para procesar H3 y agregar métricas
process_h3 <- function(data, res) {
# 1. Resumen de datos usando la función correcta: point_to_cell
  resumen <- data %>%
    mutate(cell = point_to_cell(cbind(longitud, latitud), res = res)) %>%
    group_by(cell) %>%
    summarise(
      H001A = mean(H001A, na.rm = TRUE),
      J000A = mean(J000A, na.rm = TRUE),
      P000A = mean(P000A, na.rm = TRUE),
      .groups = "drop"
    )
  
  # 2. Convertir los índices (cell) a polígonos usando la función correcta: cell_to_polygon
  # cell_to_polygon toma el vector de celdas y devuelve una lista de polígonos
  poligonos <- cell_to_polygon(resumen$cell)
  
  # 3. Convertir a sf
  # cell_to_polygon usualmente devuelve un sfc (simple feature collection)
  resultado <- st_sf(resumen, geometry = poligonos)
  
  # Asegurar el sistema de coordenadas
  st_crs(resultado) <- 4326
  
  return(resultado)
  }

# 4. Ejecución para resoluciones 7, 8 y 9
res1 <- process_h3(df, 4)
```

    Assuming columns 1 and 2 contain x, y coordinates in EPSG:4326

``` r
res2 <- process_h3(df, 5)
```

    Assuming columns 1 and 2 contain x, y coordinates in EPSG:4326

``` r
res3 <- process_h3(df, 6)
```

    Assuming columns 1 and 2 contain x, y coordinates in EPSG:4326

``` r
library(ggplot2)



ggplot() +
  geom_sf(data = res1, aes(fill = 100*H001A), alpha = 0.4) +# Capa 1: Base (ej. Resolución 7)
  geom_sf(data = res2, aes(fill = 100*H001A), alpha = 0.6) +# Capa 2: Intermedia (ej. Resolución 8)
  geom_sf(data = res3, aes(fill = 100*H001A), alpha = 0.8) +# Capa 3: Superior (ej. Resolución 9)
  geom_sf(data = mexico, fill = NA, color="green") +# Capa 1: Base (ej. Resolución 7)
  coord_sf(xlim = Xlims, ylim = Ylims) + 
  scale_fill_viridis_c(option = "magma", trans = "log10", name = "H001A (Log)") + 
  labs(title = "Comparativa de resoluciones H3")
```

    Warning in scale_fill_viridis_c(option = "magma", trans = "log10", name =
    "H001A (Log)"): log-10 transformation introduced infinite values.

![](Principal_files/figure-commonmark/unnamed-chunk-1-1.png)

``` r
# st_write(data_h3_9, "denue_h3_res9.gpkg")
```

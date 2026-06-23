# Documentación de mi Proyecto


- [<span class="toc-section-number">1</span> Simulacion
  financiera](#simulacion-financiera)
- [<span class="toc-section-number">2</span> Representacion de la
  información
  geo-referenciada.](#representacion-de-la-información-geo-referenciada)
  - [<span class="toc-section-number">2.1</span> Masa salarial
    latente](#masa-salarial-latente)
  - [<span class="toc-section-number">2.2</span> Entropía de
    Shanon](#entropía-de-shanon)
  - [<span class="toc-section-number">2.3</span> Tasa de
    Resiliencia](#tasa-de-resiliencia)

# Simulacion financiera

El objetivo de este GitHub es mostrar el uso de diferentes herramientas
computacionales en la creación de una cartera de clientes. Para lograr
esto usaremos diferentes herramientas como: - Docker para generar
contenedores con las herramientas y configuraciones necesarias. - R para
la generación de millones de registros de datos sinteticos. - PySpark y
Delta-Lake para las operaciones ETL y almacenamiento de los millones de
registros. - PyTorch, SciPy para entrenar modelos de ML y DL. - Python
para poner el modelo en producción. - Kafka para orquestar el flujo de
datos entre contenedores. El siguiente diagrama ejemplifica nuestra
configuración generada con apoyo de herramientas de IA
<img src="figures/archdiagram.png" style="width:60.0%" alt="Elephant" />

# Representacion de la información geo-referenciada.

A continuación presentamos la información agregada de distintos indices
económicos en México, Estos coeficientes nos permitirán darle realismo
nuestro producto, diferenciándolo de un producto puramente informático.

Esas variables nos permitirán perfilar clientes y productos de cualquier
tipo para simular de forma “real” una base de datos con diferentes
operaciones, transacciones y registros de interés educativo.

## Masa salarial latente

Valores cercanos a 0 indican monopolio sectorial (según códigos SCIAN).

<img src="figures/Masa%20Salarial.png" style="width:80.0%"
alt="Elephant" />

## Entropía de Shanon

Valores cercanos a 0 indican monopolio sectorial (segun codigos SCIAN).

<img src="figures/Entropia%20Shanon.png" style="width:80.0%"
alt="Elephant" />

## Tasa de Resiliencia

Valores Menores a 1 indican fragilidad crisis.

<img src="figures/Tasa%20Resiliencia.png" style="width:80.0%"
alt="Elephant" />

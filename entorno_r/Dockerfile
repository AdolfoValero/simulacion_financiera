# 1. Traer la máquina base con R y RStudio listos
FROM rocker/rstudio:4.4

# 2. Instalar herramientas del sistema que R necesita (con dependencias de igraph incluidas)
RUN apt-get update && apt-get install -y \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libglpk-dev \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    && rm -rf /var/lib/apt/lists/*

# 3. Abrir R e instalar paquetes (usando binarios de Posit para Ubuntu Jammy)
RUN R -e "options(repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/jammy/latest')); install.packages(c('dplyr', 'arrow', 'igraph'))"

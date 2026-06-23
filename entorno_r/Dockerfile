# 1. Traer la máquina base con R y RStudio listos
FROM rocker/rstudio:4.4

# 2. Instalar herramientas del sistema que R necesita (con dependencias de igraph y NETCAT incluidas)

RUN apt-get update && apt-get install -y \
git \
    netcat-openbsd \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libglpk-dev \
    libudunits2-dev \
    libproj-dev \
    libgeos-dev \
    libgdal-dev \
    libnode-dev \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# 3. Abrir R e instalar paquetes (usando binarios de Posit para Ubuntu Jammy)
RUN R -e "options(repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/jammy/latest')); install.packages(c('dplyr', 'arrow', 'igraph', 'jsonlite', 'data.table'))"
RUN R -e "options(repos = c(CRAN = 'https://packagemanager.posit.co/cran/__linux__/jammy/latest')); install.packages(c('tidyr','lubridate', 'remotes'))"
RUN R -e "install.packages(c('sf', 'V8', 'h3jsr'), type = 'source', repos = 'https://cloud.r-project.org/')"
RUN R -e "remotes::install_github('inwtlab/r-kafka')"



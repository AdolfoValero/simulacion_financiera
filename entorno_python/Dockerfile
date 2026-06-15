# 1. Traer la máquina base con Python, Jupyter y PySpark ya configurados
FROM jupyter/pyspark-notebook:latest

# 2. CAMBIAR A ROOT PARA PODER INSTALAR COSAS DEL SISTEMA
USER root

# 1. Instalar compiladores de C++ (build-essential) y cabeceras de Python
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 3. instalar modulos de python
RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir numpy cython
RUN pip install --no-cache-dir polars
RUN pip install --no-cache-dir hdbscan
RUN pip install --no-cache-dir python-dateutil

# 6. REGRESAR AL USUARIO NORMAL (Opcional pero recomendado por seguridad)
USER $NB_UID

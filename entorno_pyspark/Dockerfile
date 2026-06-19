
FROM jupyter/pyspark-notebook:latest

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir numpy cython polars hdbscan python-dateutil delta-spark

USER $NB_UID
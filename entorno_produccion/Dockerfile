# Python súper ligero (pesa ~150 MB en lugar de los 4GB de Spark)
FROM python:3.11-slim

WORKDIR /app

# Dependencias mínimas del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Instalamos el stack de Inferencia (API rápida + Polars para formato + ML)
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    polars \
    deltalake \
    fastapi \
    uvicorn \
    onnxruntime \
    scikit-learn

# ONNX es el estándar de la industria para inferencia ligera de Deep Learning en CPU/GPU
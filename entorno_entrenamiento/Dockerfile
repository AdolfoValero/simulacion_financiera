# Usa la imagen de NVIDIA que descargaste
FROM nvcr.io/nvidia/pytorch:26.05-py3

# Instalamos las herramientas necesarias para leer el Data Lake y entrenar
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    deltalake \
    polars \
    scipy \
    scikit-learn \
    matplotlib \
    jupyterlab

WORKDIR /workspace
# Comando por defecto para iniciar Jupyter Lab
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser"]
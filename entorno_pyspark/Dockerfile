FROM jupyter/pyspark-notebook:latest

USER root

# 1. Instalar Java 21, curl y herramientas esenciales
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    build-essential \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2. ELIMINAR FÍSICAMENTE EL SPARK VIEJO QUE CAUSA EL CRASH
RUN rm -rf /usr/local/spark

# 3. Apuntar variables al entorno nativo de Java 21
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# 4. Instalar PySpark 4.0.1 y Delta 4.3.0 de forma limpia
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    numpy \
    cython \
    polars \
    hdbscan \
    python-dateutil \
    pyspark==4.0.1 \
    delta-spark==4.0.1

# 5. Descarga de JARs en site-packages de pip (como root)
RUN PYSPARK_JARS=$(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    curl -o $PYSPARK_JARS/spark-sql-kafka-0-10_2.13-4.0.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.1/spark-sql-kafka-0-10_2.13-4.0.1.jar && \
    curl -o $PYSPARK_JARS/spark-token-provider-kafka-0-10_2.13-4.0.1.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.1/spark-token-provider-kafka-0-10_2.13-4.0.1.jar && \
    curl -o $PYSPARK_JARS/kafka-clients-3.7.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.1/kafka-clients-3.7.1.jar && \
    curl -o $PYSPARK_JARS/commons-pool2-2.12.0.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \
    curl -o $PYSPARK_JARS/delta-spark_2.13-4.0.1.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.1/delta-spark_2.13-4.0.1.jar && \
    curl -o $PYSPARK_JARS/delta-storage-4.0.1.jar https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.1/delta-storage-4.0.1.jar

# 6. AUTORRANQUE DEL THRIFT SERVER
# Creamos un script que Jupyter ejecutará automáticamente al encender el contenedor.
# Localizamos dinámicamente dónde instaló pip a PySpark y ejecutamos el start-thriftserver.sh
RUN mkdir -p /usr/local/bin/start-notebook.d && \
    echo '#!/bin/bash\n\
export SPARK_HOME=$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")\n\
bash $SPARK_HOME/sbin/start-thriftserver.sh\n' > /usr/local/bin/start-notebook.d/auto_thrift.sh && \
    chmod +x /usr/local/bin/start-notebook.d/auto_thrift.sh

USER $NB_USER





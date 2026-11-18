ARG SPARK_VERSION=3.5.5
FROM apache/spark:${SPARK_VERSION}

USER root
ARG SPARK_VERSION
ENV PIP_BREAK_SYSTEM_PACKAGES=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3-pip \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install --no-cache-dir pyspark==${SPARK_VERSION} jupyterlab pandas matplotlib

WORKDIR /workspace
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=sahlpay"]
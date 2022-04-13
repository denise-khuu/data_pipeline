FROM apache/airflow:2.2.5
USER root
RUN  apt-get update \
  && apt-get install -y wget \
  && apt-get install -y gzip \
  && rm -rf /var/lib/apt/lists/*
USER airflow
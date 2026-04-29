FROM apache/spark:3.5.1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    mlflow==2.16.2 \
    redis==5.1.1 \
    psycopg[binary]==3.2.3 \
    sentence-transformers==3.0.1

RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /opt/spark/work-dir

FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.1.0 \
    pandas==2.2.0 \
    numpy==1.26.4 \
    pyarrow==15.0.0

WORKDIR /app
COPY . .

CMD ["python", "src/data_generator.py"]
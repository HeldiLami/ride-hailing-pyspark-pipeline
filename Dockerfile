FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python
ENV SPARK_LOCAL_IP=127.0.0.1

WORKDIR /app
COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY . .

CMD ["python", "-m", "src.pipeline"]

FROM apache/spark:4.1.2-python3
USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10
COPY streaming/*.py /opt/spark-app/
WORKDIR /opt/spark-app
USER spark

FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.15.0
COPY monitoring/lag_reporter/lag_reporter.py /app/lag_reporter.py
CMD ["python", "lag_reporter.py"]

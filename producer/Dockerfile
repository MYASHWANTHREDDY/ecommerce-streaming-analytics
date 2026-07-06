FROM python:3.12-slim
WORKDIR /app
COPY producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY producer/ /app/producer/
COPY data/ /app/data/
WORKDIR /app/producer
CMD ["python", "producer.py"]

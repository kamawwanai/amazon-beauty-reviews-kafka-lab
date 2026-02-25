FROM python:3.12-slim
WORKDIR /app
COPY requirements-ml.txt .
RUN pip install --no-cache-dir -r requirements-ml.txt
COPY . .
ENV PYTHONPATH=/app/producers:/app/consumers:/app/config

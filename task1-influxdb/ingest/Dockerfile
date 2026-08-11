FROM python:3.12.13-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

COPY ingest.py .
COPY test_ingest.py .

CMD ["python", "/app/ingest.py"]

FROM python:3.11-slim

WORKDIR /app

# DocumentDB TLS certificate
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -sS -o /app/global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ .

# Jobs directory for pipeline output (mount a volume in production)
RUN mkdir -p /app/jobs

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# =============================================================================
# Dockerfile — ShardShop (backend base)
# =============================================================================
# Imagen del panel FastAPI. Se construye como servicio `app` de docker-compose.
# Sin ORM: la app habla SQL explícito contra los dos pools (Citus y usuarios).
# =============================================================================
FROM python:3.11-slim

# No escribimos .pyc y mostramos logs sin buffer (útiles con `docker compose logs`).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Copiamos primero los requisitos para aprovechar la caché de capas.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código de la aplicación.
COPY app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

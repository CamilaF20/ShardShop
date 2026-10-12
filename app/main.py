"""Punto de entrada de la API ShardShop (FastAPI).

Ilustra la convivencia de los dos tipos de sharding desde el backend: la app
mantiene dos pools independientes (Citus y `db_usuarios`) y expone un `/health`
que verifica ambos dominios y lista los workers activos del cluster.

Concepto de Citus: `citus_get_active_worker_nodes()` consulta la metadata del
cluster para saber cuántos workers participan del sharding horizontal.
"""

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from psycopg_pool import AsyncConnectionPool

from app.db import pool_citus, pool_usuarios
from app.routers import admin, demo_vertical, pedidos, productos, shards, usuarios
from app.schemas import DatabaseStatus, HealthResponse, WorkerNode


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Abre ambos pools al arrancar y los cierra al apagar la app.

    Es el ciclo de vida de las conexiones: un pool por dominio del sharding
    vertical, abiertos de forma explícita (se crearon con `open=False`).
    """
    await pool_citus.open(wait=True, timeout=30)
    await pool_usuarios.open(wait=True, timeout=30)
    try:
        yield
    finally:
        await pool_citus.close()
        await pool_usuarios.close()


app = FastAPI(title="ShardShop", version="0.1.0", lifespan=lifespan)

# --- Routers de dominio -----------------------------------------------------
for _router in (
    usuarios.router,
    demo_vertical.router,
    productos.router,
    pedidos.router,
    shards.router,
    admin.router,
):
    app.include_router(_router)


async def _check(pool: AsyncConnectionPool, sql: str) -> DatabaseStatus:
    """Ejecuta `sql` para comprobar que un pool responde y mide la latencia."""
    start = time.perf_counter()
    try:
        async with pool.connection() as conn:
            await conn.execute(sql)
        latency_ms = (time.perf_counter() - start) * 1000
        return DatabaseStatus(ok=True, latency_ms=round(latency_ms, 2))
    except Exception as exc:  # noqa: BLE001 - se reporta el error tal cual
        return DatabaseStatus(ok=False, error=str(exc))


@app.get("/health", response_model=HealthResponse, tags=["infra"])
async def health() -> HealthResponse:
    """Estado de los dos dominios y workers activos del cluster Citus.

    Ilustra el sharding vertical (dos bases independientes) y el horizontal
    (los workers que almacenan los shards). Si una base falla, se reporta como
    `error` sin tumbar el endpoint.
    """
    citus_status = await _check(pool_citus, "SELECT 1")
    usuarios_status = await _check(pool_usuarios, "SELECT 1")

    workers: list[WorkerNode] = []
    if citus_status.ok:
        async with pool_citus.connection() as conn:
            cur = await conn.execute(
                "SELECT node_name, node_port FROM citus_get_active_worker_nodes() "
                "ORDER BY node_name"
            )
            workers = [
                WorkerNode(node_name=row[0], node_port=row[1])
                for row in await cur.fetchall()
            ]

    overall = "ok" if citus_status.ok and usuarios_status.ok else "degraded"
    return HealthResponse(
        status=overall,
        citus=citus_status,
        usuarios=usuarios_status,
        workers=workers,
    )


# --- Frontend estático (panel de la Fase 8) ----------------------------------
# Debe montarse AL FINAL: un Mount en "/" captura todas las rutas, así que si se
# registra antes taparía `/health` (y cualquier endpoint añadido después). Los
# routers y /docs se definen antes y por eso siguen accesibles.
_static_dir = Path(__file__).parent / "static"
if _static_dir.is_dir():
    app.mount("/", StaticFiles(directory=_static_dir, html=True), name="static")

"""Endpoint de demostración del beneficio del sharding VERTICAL.

Mide `N` lecturas que solo tocan las columnas CALIENTES (`usuarios`) contra `N`
lecturas que hacen JOIN con las columnas PESADAS (`usuarios_perfil`: biografia,
foto bytea, preferencias jsonb). El objetivo es evidenciar que aislar las
columnas pesadas reduce el I/O y el volumen de datos transferido.

Concepto de sharding vertical (por columnas): no lo hace el motor por sí solo;
se logra por diseño separando las columnas en tablas distintas del MISMO
servidor (`db_usuarios`).
"""

import time
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from app.db import get_usuarios
from app.schemas import DemoVerticalResult

router = APIRouter(prefix="/demo", tags=["demo"])

# Consulta "caliente": solo columnas ligeras.
_SQL_CALIENTE = "SELECT id, nombre, email, creado_en FROM usuarios WHERE id = %s"

# Consulta "pesada": trae además biografia (text), foto (bytea) y preferencias (jsonb).
_SQL_PESADA = (
    "SELECT u.id, u.nombre, u.email, u.creado_en, "
    "       p.biografia, p.foto, p.preferencias "
    "FROM usuarios u JOIN usuarios_perfil p ON p.usuario_id = u.id "
    "WHERE u.id = %s"
)


def _row_bytes(row: dict[str, Any]) -> int:
    """Aproxima los bytes transferidos de una fila (suma de sus columnas)."""
    total = 0
    for value in row.values():
        if value is None:
            continue
        if isinstance(value, (bytes, bytearray)):
            total += len(value)
        else:
            total += len(str(value).encode("utf-8"))
    return total


@router.get("/vertical", response_model=DemoVerticalResult)
async def demo_vertical(
    n: int = Query(200, ge=1, le=10000),
    conn: AsyncConnection = Depends(get_usuarios),
) -> DemoVerticalResult:
    """Compara N lecturas calientes vs N lecturas con columnas pesadas.

    Devuelve tiempos, bytes aproximados y el factor de mejora (>1 significa que
    las lecturas calientes son más rápidas). 404 si aún no hay usuarios.
    """
    # Elegimos un usuario representativo (el de menor id).
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT id FROM usuarios ORDER BY id LIMIT 1")
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="No hay usuarios para medir; crea uno con POST /usuarios",
        )
    usuario_id = row["id"]

    # --- N lecturas CALIENTES (solo `usuarios`) ---
    inicio = time.perf_counter()
    bytes_calientes = 0
    for _ in range(n):
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_SQL_CALIENTE, (usuario_id,))
            fila = await cur.fetchone()
        bytes_calientes += _row_bytes(fila)
    calientes_ms = (time.perf_counter() - inicio) * 1000

    # --- N lecturas PESADAS (JOIN con `usuarios_perfil`) ---
    inicio = time.perf_counter()
    bytes_pesados = 0
    for _ in range(n):
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_SQL_PESADA, (usuario_id,))
            fila = await cur.fetchone()
        bytes_pesados += _row_bytes(fila)
    pesadas_ms = (time.perf_counter() - inicio) * 1000

    factor = (pesadas_ms / calientes_ms) if calientes_ms > 0 else 0.0

    return DemoVerticalResult(
        n=n,
        usuario_id=usuario_id,
        lecturas_calientes_ms=round(calientes_ms, 3),
        lecturas_pesadas_ms=round(pesadas_ms, 3),
        factor_mejora=round(factor, 3),
        bytes_calientes=bytes_calientes,
        bytes_pesados=bytes_pesados,
    )

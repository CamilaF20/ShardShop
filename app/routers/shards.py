"""Endpoints de evidencia del sharding horizontal.

Hacen visible DÓNDE viven las filas y CÓMO enruta Citus las consultas:

- `/shards/distribucion`: filas por shard cruzando `run_command_on_shards` (que
  ejecuta la consulta EN cada shard) con la vista `citus_shards` (que dice en qué
  nodo vive cada shard), más un resumen por worker.
- `/shards/ubicacion/{cliente_id}`: en qué shard/worker cae un cliente usando
  `get_shard_id_for_distribution_column`.
- `/shards/explain`: `EXPLAIN` de una consulta enrutada (Task Count 1) frente a
  una agregación global (Task Count = nº de shards).

Sin ORM: SQL explícito con psycopg 3.
"""

import re

from fastapi import APIRouter, Depends, Query
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from app.db import get_citus
from app.schemas import (
    DistribucionResponse,
    ExplainParte,
    ExplainResponse,
    NodoResumen,
    ShardFilas,
    UbicacionShard,
)

router = APIRouter(prefix="/shards", tags=["shards"])

# `EXPLAIN` de una consulta distribuida incluye una línea "Task Count: N".
_TASK_COUNT_RE = re.compile(r"Task Count:\s*(\d+)")


@router.get("/distribucion", response_model=DistribucionResponse)
async def distribucion(
    conn: AsyncConnection = Depends(get_citus),
) -> DistribucionResponse:
    """Filas de `pedidos` por shard y por nodo.

    Concepto de Citus: `run_command_on_shards` envía `SELECT count(*) FROM %s` a
    CADA shard; se cruza con `citus_shards` (shardid -> nodename) para saber en
    qué worker vive cada shard. El resultado evidencia el reparto horizontal.

    Nota: el `%s` es un marcador de Citus, no de psycopg. Como esta consulta no
    recibe parámetros (`params=None`), psycopg no lo interpreta y llega literal.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT r.shardid, cs.nodename, r.result::bigint AS filas "
            "FROM run_command_on_shards('pedidos', $$SELECT count(*) FROM %s$$) r "
            "JOIN citus_shards cs "
            "  ON cs.shardid = r.shardid "
            " AND cs.table_name = 'pedidos'::regclass "
            "WHERE r.success "
            "ORDER BY cs.nodename, r.shardid"
        )
        shards = await cur.fetchall()

    total = sum(row["filas"] for row in shards)

    # Resumen por worker (porcentaje del total) calculado en Python.
    acumulado: dict[str, int] = {}
    for row in shards:
        acumulado[row["nodename"]] = acumulado.get(row["nodename"], 0) + row["filas"]

    por_nodo = [
        NodoResumen(
            nodename=nodo,
            filas=filas,
            porcentaje=round(100.0 * filas / total, 2) if total else 0.0,
        )
        for nodo, filas in sorted(acumulado.items())
    ]

    return DistribucionResponse(
        tabla="pedidos",
        total_filas=total,
        shards=[ShardFilas(**row) for row in shards],
        por_nodo=por_nodo,
    )


@router.get("/ubicacion/{cliente_id}", response_model=UbicacionShard)
async def ubicacion(
    cliente_id: int,
    conn: AsyncConnection = Depends(get_citus),
) -> UbicacionShard:
    """Shard y worker donde cae un cliente.

    Concepto de Citus: `get_shard_id_for_distribution_column` aplica la misma
    función hash que usa el sharding para decir a qué shard va un valor de la
    clave de distribución (el mismo que se usará al insertar/consultar).
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT get_shard_id_for_distribution_column('pedidos', %s) AS shardid",
            (cliente_id,),
        )
        fila = await cur.fetchone()
        await cur.execute(
            "SELECT nodename FROM citus_shards "
            "WHERE shardid = %s AND table_name = 'pedidos'::regclass",
            (fila["shardid"],),
        )
        nodo = await cur.fetchone()

    return UbicacionShard(
        cliente_id=cliente_id,
        shardid=fila["shardid"],
        nodename=nodo["nodename"] if nodo else "desconocido",
    )


async def _explain(cur, titulo: str, sql_texto: str) -> ExplainParte:
    """Ejecuta `EXPLAIN` y extrae el plan textual y su `Task Count`."""
    await cur.execute(f"EXPLAIN {sql_texto}")
    filas = await cur.fetchall()
    plan = "\n".join(fila[0] for fila in filas)
    coincidencia = _TASK_COUNT_RE.search(plan)
    return ExplainParte(
        titulo=titulo,
        sql=sql_texto,
        plan=plan,
        task_count=int(coincidencia.group(1)) if coincidencia else None,
    )


@router.get("/explain", response_model=ExplainResponse)
async def explain(
    cliente_id: int = Query(42),
    conn: AsyncConnection = Depends(get_citus),
) -> ExplainResponse:
    """Compara el plan enrutado vs el distribuido.

    Concepto de Citus: con `WHERE cliente_id = X` el planificador poda y genera
    UNA tarea (Task Count: 1); una agregación sin filtro por la clave de
    distribución lanza una tarea por shard (Task Count: 32).
    """
    async with conn.cursor() as cur:
        enrutada = await _explain(
            cur,
            "Consulta enrutada por cliente_id",
            f"SELECT * FROM pedidos WHERE cliente_id = {cliente_id}",
        )
        global_ = await _explain(
            cur,
            "Agregación global",
            "SELECT count(*), sum(total) FROM pedidos",
        )
    return ExplainResponse(consultas=[enrutada, global_])

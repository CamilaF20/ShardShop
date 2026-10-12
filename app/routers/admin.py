"""Endpoints de administración del cluster Citus.

- `/admin/nodos`: lista los nodos registrados (metadata `pg_dist_node`).
- `/admin/rebalance`: reparte los shards de `pedidos` entre los workers con
  `rebalance_table_shards`, usando `shard_transfer_mode := 'block_writes'` para
  NO exigir `wal_level=logical` (ver notas técnicas, sección 5 de PLAN.md).

Sin ORM: SQL explícito con psycopg 3.
"""

from fastapi import APIRouter, Depends
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from app.db import get_citus
from app.schemas import NodoInfo, RebalanceResponse

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/nodos", response_model=list[NodoInfo])
async def nodos(
    conn: AsyncConnection = Depends(get_citus),
) -> list[NodoInfo]:
    """Lista los nodos registrados en el cluster (metadata `pg_dist_node`).

    Concepto de Citus: `pg_dist_node` es la fuente autoritativa de los nodos; a
    diferencia de `citus_get_active_worker_nodes`, incluye también los inactivos.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT nodename, nodeport, isactive FROM pg_dist_node ORDER BY nodename"
        )
        rows = await cur.fetchall()
    return [NodoInfo(**row) for row in rows]


@router.post("/rebalance", response_model=RebalanceResponse)
async def rebalance(
    conn: AsyncConnection = Depends(get_citus),
) -> RebalanceResponse:
    """Rebalancea los shards de `pedidos` entre los workers.

    Concepto de Citus: `rebalance_table_shards` mueve shards para igualar la carga.
    Se usa `shard_transfer_mode := 'block_writes'`: las tablas co-localizadas
    (`detalle_pedidos`) se mueven juntas y no se requiere replicación lógica.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT rebalance_table_shards("
            "'pedidos', shard_transfer_mode := 'block_writes')"
        )
    await conn.commit()
    return RebalanceResponse(
        tabla="pedidos",
        shard_transfer_mode="block_writes",
        resultado="ok",
    )

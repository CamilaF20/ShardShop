"""API de productos — REFERENCE TABLE de Citus.

`productos` es una tabla de referencia (sharding horizontal "replicado"): Citus
mantiene una copia completa en cada worker del cluster. Así, cualquier JOIN de
`detalle_pedidos` con `productos` puede resolverse localmente en el shard, sin
mover datos entre nodos.

Sin ORM: SQL explícito con psycopg 3.
"""

from fastapi import APIRouter, Depends
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from app.db import get_citus
from app.schemas import ProductoCreate, ProductoOut

router = APIRouter(prefix="/productos", tags=["productos"])


@router.post("", response_model=ProductoOut, status_code=201)
async def crear_producto(
    payload: ProductoCreate,
    conn: AsyncConnection = Depends(get_citus),
) -> ProductoOut:
    """Crea un producto en la reference table.

    Concepto de Citus: al insertar en una reference table, el coordinator
    propaga la fila a TODOS los workers, de modo que la copia queda consistente
    en cada nodo.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "INSERT INTO productos (nombre, precio, stock) VALUES (%s, %s, %s) "
            "RETURNING id, nombre, precio, stock",
            (payload.nombre, payload.precio, payload.stock),
        )
        row = await cur.fetchone()
    await conn.commit()
    return ProductoOut(**row)


@router.get("", response_model=list[ProductoOut])
async def listar_productos(
    conn: AsyncConnection = Depends(get_citus),
) -> list[ProductoOut]:
    """Lista el catálogo completo (reference table).

    Concepto de Citus: una reference table se consulta desde cualquier nodo sin
    necesidad de conocer la clave de distribución.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, nombre, precio, stock FROM productos ORDER BY id"
        )
        rows = await cur.fetchall()
    return [ProductoOut(**row) for row in rows]

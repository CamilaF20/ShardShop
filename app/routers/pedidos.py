"""API de pedidos y estadísticas — SHARDING HORIZONTAL en Citus.

`pedidos` y `detalle_pedidos` están distribuidas por `cliente_id` y CO-LOCALIZADAS
(mismo `colocationid`). Todas las consultas de este router filtran o insertan por
`cliente_id`, aprovechando la poda de shards y la localidad de los JOIN.

Sin ORM: SQL explícito con psycopg 3.
"""

from fastapi import APIRouter, Depends, HTTPException
from psycopg import AsyncConnection
from psycopg.rows import dict_row

from app.db import get_citus, get_usuarios
from app.schemas import (
    EstadisticasResponse,
    PedidoCreate,
    PedidoItemOut,
    PedidoOut,
    TopProducto,
)

router = APIRouter(tags=["pedidos"])


@router.post("/pedidos", response_model=PedidoOut, status_code=201)
async def crear_pedido(
    payload: PedidoCreate,
    citus: AsyncConnection = Depends(get_citus),
    usuarios: AsyncConnection = Depends(get_usuarios),
) -> PedidoOut:
    """Crea un pedido con su detalle en UNA SOLA TRANSACCIÓN sobre Citus.

    Pasos:
      1. Valida la existencia del cliente en `db_usuarios` (404 si no existe);
         no hay FK posible entre bases, así que la app es quien garantiza esto.
      2. Lee precios de la reference table `productos` y valida que existan (404).
      3. En una transacción sobre Citus inserta `pedidos` (RETURNING id), luego
         `detalle_pedidos` con el MISMO `cliente_id`, y actualiza el total.

    ¿Por qué NO hay 2PC (two-phase commit)? Porque `pedidos` y `detalle_pedidos`
    comparten la clave de distribución `cliente_id` y están co-localizadas: todas
    las escrituras caen en el MISMO shard, por lo que Citus resuelve la transacción
    de forma local (commit en un solo nodo, sin coordinador distribuido ni 2PC).
    """
    cliente_id = payload.cliente_id

    # 1) El cliente vive en OTRA base (sharding vertical): se valida aparte.
    async with usuarios.cursor() as cur:
        await cur.execute("SELECT 1 FROM usuarios WHERE id = %s", (cliente_id,))
        if await cur.fetchone() is None:
            raise HTTPException(
                status_code=404, detail=f"El cliente {cliente_id} no existe"
            )

    # 2) Precios desde la reference table (replicada en todos los workers).
    producto_ids = [item.producto_id for item in payload.items]
    async with citus.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, nombre, precio FROM productos WHERE id = ANY(%s)",
            (producto_ids,),
        )
        productos = {row["id"]: row for row in await cur.fetchall()}

    faltantes = [pid for pid in producto_ids if pid not in productos]
    if faltantes:
        raise HTTPException(
            status_code=404, detail=f"Productos inexistentes: {faltantes}"
        )

    # 3) Transacción en Citus: pedido + detalle + total, todo al mismo shard.
    total = 0.0
    items_out: list[PedidoItemOut] = []
    for item in payload.items:
        producto = productos[item.producto_id]
        precio = float(producto["precio"])
        subtotal = precio * item.cantidad
        total += subtotal
        items_out.append(
            PedidoItemOut(
                producto_id=item.producto_id,
                nombre=producto["nombre"],
                cantidad=item.cantidad,
                precio_unitario=precio,
                subtotal=subtotal,
            )
        )

    async with citus.cursor(row_factory=dict_row) as cur:
        # Insert del pedido; RETURNING id da el id local al shard del cliente.
        await cur.execute(
            "INSERT INTO pedidos (cliente_id, estado, total) "
            "VALUES (%s, 'pendiente', 0) "
            "RETURNING id, cliente_id, fecha, estado, total",
            (cliente_id,),
        )
        pedido = await cur.fetchone()

        # Detalle con el MISMO cliente_id => misma fila de pedido y mismo shard.
        for item in payload.items:
            await cur.execute(
                "INSERT INTO detalle_pedidos "
                "(cliente_id, pedido_id, producto_id, cantidad, precio_unitario) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    cliente_id,
                    pedido["id"],
                    item.producto_id,
                    item.cantidad,
                    productos[item.producto_id]["precio"],
                ),
            )

        # Actualización del total (filtra por la clave de distribución: shard único).
        await cur.execute(
            "UPDATE pedidos SET total = %s WHERE cliente_id = %s AND id = %s",
            (total, cliente_id, pedido["id"]),
        )
    # Commit único: al ser todo local a un shard, no hace falta 2PC.
    await citus.commit()

    pedido["total"] = total
    pedido["items"] = items_out
    return PedidoOut(**pedido)


@router.get("/clientes/{cliente_id}/pedidos", response_model=list[PedidoOut])
async def pedidos_de_cliente(
    cliente_id: int,
    citus: AsyncConnection = Depends(get_citus),
) -> list[PedidoOut]:
    """Pedidos de un cliente con su detalle (JOIN co-localizado).

    Concepto de Citus: al filtrar por `cliente_id`, la consulta se poda a UN solo
    shard; el JOIN entre `pedidos` y `detalle_pedidos` es local (mismo shard) y
    `productos` se une como reference table.
    """
    async with citus.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT p.id, p.cliente_id, p.fecha, p.estado, p.total, "
            "       d.producto_id, pr.nombre AS nombre, d.cantidad, "
            "       d.precio_unitario "
            "FROM pedidos p "
            "JOIN detalle_pedidos d "
            "  ON d.cliente_id = p.cliente_id AND d.pedido_id = p.id "
            "JOIN productos pr ON pr.id = d.producto_id "
            "WHERE p.cliente_id = %s "
            "ORDER BY p.id, d.id",
            (cliente_id,),
        )
        rows = await cur.fetchall()

    pedidos: dict[int, PedidoOut] = {}
    for row in rows:
        pedido = pedidos.get(row["id"])
        if pedido is None:
            pedido = PedidoOut(
                id=row["id"],
                cliente_id=row["cliente_id"],
                fecha=row["fecha"],
                estado=row["estado"],
                total=float(row["total"]),
                items=[],
            )
            pedidos[row["id"]] = pedido
        precio = float(row["precio_unitario"])
        pedido.items.append(
            PedidoItemOut(
                producto_id=row["producto_id"],
                nombre=row["nombre"],
                cantidad=row["cantidad"],
                precio_unitario=precio,
                subtotal=precio * row["cantidad"],
            )
        )
    return list(pedidos.values())


@router.get("/estadisticas", response_model=EstadisticasResponse)
async def estadisticas(
    citus: AsyncConnection = Depends(get_citus),
) -> EstadisticasResponse:
    """Agregados globales del cluster (agregación en TODOS los shards).

    Concepto de Citus: `count`, `sum` y `avg` sin filtro por la clave de
    distribución obligan a un plan distribuido: el coordinator lanza una tarea por
    shard (Task Count = 32) y combina los resultados parciales.
    """
    async with citus.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT count(*) AS total_pedidos, "
            "       coalesce(sum(total), 0) AS ingresos, "
            "       coalesce(avg(total), 0) AS ticket_promedio "
            "FROM pedidos"
        )
        resumen = await cur.fetchone()

        await cur.execute(
            "SELECT d.producto_id, pr.nombre AS nombre, "
            "       sum(d.cantidad) AS unidades, "
            "       sum(d.cantidad * d.precio_unitario) AS ingresos "
            "FROM detalle_pedidos d "
            "JOIN productos pr ON pr.id = d.producto_id "
            "GROUP BY d.producto_id, pr.nombre "
            "ORDER BY unidades DESC, d.producto_id "
            "LIMIT 5"
        )
        top = await cur.fetchall()

    return EstadisticasResponse(
        total_pedidos=resumen["total_pedidos"],
        ingresos=float(resumen["ingresos"]),
        ticket_promedio=float(resumen["ticket_promedio"]),
        top_productos=[
            TopProducto(
                producto_id=row["producto_id"],
                nombre=row["nombre"],
                unidades=int(row["unidades"]),
                ingresos=float(row["ingresos"]),
            )
            for row in top
        ],
    )

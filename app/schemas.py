"""Modelos Pydantic de ShardShop.

Incluye la respuesta de `GET /health` (estado de los dos dominios del sharding)
y los esquemas de usuarios, que ilustran el sharding VERTICAL: se distinguen las
columnas calientes (`UsuarioOut`) de las pesadas (`UsuarioPerfilOut`).
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DatabaseStatus(BaseModel):
    """Estado de un destino de base de datos."""

    ok: bool
    latency_ms: float | None = None
    error: str | None = None


class WorkerNode(BaseModel):
    """Un worker registrado en el cluster Citus."""

    node_name: str
    node_port: int


class HealthResponse(BaseModel):
    """Respuesta de `GET /health`."""

    status: str
    citus: DatabaseStatus
    usuarios: DatabaseStatus
    workers: list[WorkerNode]


# --- Usuarios (sharding VERTICAL) -------------------------------------------


class UsuarioCreate(BaseModel):
    """Alta de usuario: columnas calientes + pesadas (estas en `usuarios_perfil`)."""

    nombre: str
    email: str
    biografia: str | None = None
    foto_base64: str | None = None
    preferencias: dict[str, Any] | None = None


class UsuarioOut(BaseModel):
    """Columnas CALIENTES de `usuarios` (sin tocar `usuarios_perfil`)."""

    id: int
    nombre: str
    email: str
    creado_en: datetime


class UsuarioPerfilOut(UsuarioOut):
    """Vista completa: columnas calientes + pesadas (requiere JOIN)."""

    biografia: str | None = None
    preferencias: dict[str, Any] | None = None
    foto_base64: str | None = None


class DemoVerticalResult(BaseModel):
    """Resultado de `GET /demo/vertical`: calientes vs pesadas."""

    n: int
    usuario_id: int | None = None
    lecturas_calientes_ms: float
    lecturas_pesadas_ms: float
    factor_mejora: float
    bytes_calientes: int
    bytes_pesados: int


# --- Productos (reference table en Citus) -----------------------------------


class ProductoCreate(BaseModel):
    """Alta de producto (se replica en cada worker como reference table)."""

    nombre: str
    precio: float
    stock: int = 0


class ProductoOut(BaseModel):
    """Producto del catálogo."""

    id: int
    nombre: str
    precio: float
    stock: int


# --- Pedidos (tablas distribuidas por cliente_id) ---------------------------


class PedidoItemIn(BaseModel):
    """Línea de un pedido entrante. `cantidad` debe ser > 0 (si no, 422)."""

    producto_id: int
    cantidad: int = Field(gt=0)


class PedidoCreate(BaseModel):
    """Pedido entrante: cliente + al menos un item."""

    cliente_id: int
    items: list[PedidoItemIn] = Field(min_length=1)


class PedidoItemOut(BaseModel):
    """Línea de pedido ya resuelta con nombre y precio del producto."""

    producto_id: int
    nombre: str
    cantidad: int
    precio_unitario: float
    subtotal: float


class PedidoOut(BaseModel):
    """Pedido con su detalle."""

    id: int
    cliente_id: int
    fecha: datetime
    estado: str
    total: float
    items: list[PedidoItemOut]


# --- Estadísticas globales (agregación en todos los shards) -----------------


class TopProducto(BaseModel):
    """Producto dentro del ranking de más vendidos."""

    producto_id: int
    nombre: str
    unidades: int
    ingresos: float


class EstadisticasResponse(BaseModel):
    """Agregados globales del cluster Citus."""

    total_pedidos: int
    ingresos: float
    ticket_promedio: float
    top_productos: list[TopProducto]


# --- Shards y administración (evidencia del sharding) -----------------------


class ShardFilas(BaseModel):
    """Filas almacenadas en un shard concreto."""

    shardid: int
    nodename: str
    filas: int


class NodoResumen(BaseModel):
    """Resumen de filas por worker con su porcentaje del total."""

    nodename: str
    filas: int
    porcentaje: float


class DistribucionResponse(BaseModel):
    """Distribución de `pedidos` por shard y por nodo."""

    tabla: str
    total_filas: int
    shards: list[ShardFilas]
    por_nodo: list[NodoResumen]


class UbicacionShard(BaseModel):
    """Shard y worker donde cae un cliente según la clave de distribución."""

    cliente_id: int
    shardid: int
    nodename: str


class ExplainParte(BaseModel):
    """Un `EXPLAIN` con su plan textual y el `Task Count` extraído."""

    titulo: str
    sql: str
    plan: str
    task_count: int | None


class ExplainResponse(BaseModel):
    """Comparación de planes: consulta enrutada vs agregación global."""

    consultas: list[ExplainParte]


class NodoInfo(BaseModel):
    """Nodo registrado en el cluster Citus."""

    nodename: str
    nodeport: int
    isactive: bool


class RebalanceResponse(BaseModel):
    """Resultado del rebalanceo de shards."""

    tabla: str
    shard_transfer_mode: str
    resultado: str

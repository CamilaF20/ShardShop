"""Pools de conexión a los dos dominios de datos.

Ilustra la separación FÍSICA del sharding vertical: no hay una única conexión
"a la base", sino dos destinos con ciclos de vida propios.

- `pool_citus`    -> coordinator Citus (sharding horizontal).
- `pool_usuarios` -> `db_usuarios` (sharding vertical).

Se usa `psycopg_pool.AsyncConnectionPool` de forma consistente. Los pools se
crean cerrados (`open=False`) y `main.py` los abre/cierra en el `lifespan`.
La app NO usa ORM: se ejecuta SQL explícito sobre las conexiones.
"""

from collections.abc import AsyncIterator

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from app.config import settings

# `open=False`: la apertura se hace explícitamente en el lifespan de FastAPI.
pool_citus: AsyncConnectionPool = AsyncConnectionPool(
    conninfo=settings.citus_conninfo(),
    open=False,
)

pool_usuarios: AsyncConnectionPool = AsyncConnectionPool(
    conninfo=settings.usuarios_conninfo(),
    open=False,
)


async def get_citus() -> AsyncIterator[AsyncConnection]:
    """Dependencia de FastAPI: presta una conexión al coordinator de Citus.

    Concepto de sharding: las consultas horizontales (pedidos/productos) se
    enrutan aquí; Citus decide qué shard(s) tocar según la clave de distribución.
    """
    async with pool_citus.connection() as conn:
        yield conn


async def get_usuarios() -> AsyncIterator[AsyncConnection]:
    """Dependencia de FastAPI: presta una conexión a `db_usuarios`.

    Concepto de sharding vertical: el dominio de identidad se consulta en una
    base distinta e independiente del cluster Citus.
    """
    async with pool_usuarios.connection() as conn:
        yield conn

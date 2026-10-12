"""API de usuarios — ilustra el SHARDING VERTICAL.

Todo este router habla contra `db_usuarios` (pool_usuarios), una base PostgreSQL
INDEPENDIENTE del cluster Citus. El sharding vertical se manifiesta de dos formas:

- Por dominio: los usuarios viven en su propio servidor, separados de las tablas
  transaccionales (pedidos/productos) que viven en el cluster Citus.
- Por columnas: las columnas calientes (`usuarios`) se separan de las pesadas
  (`usuarios_perfil`: biografia, foto bytea, preferencias jsonb).

Sin ORM: SQL explícito con psycopg 3.
"""

import base64
import binascii

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import AsyncConnection
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from app.db import get_usuarios
from app.schemas import UsuarioCreate, UsuarioOut, UsuarioPerfilOut

router = APIRouter(prefix="/usuarios", tags=["usuarios"])


def _decode_foto(foto_base64: str | None) -> bytes | None:
    """Decodifica la foto base64 a bytes para la columna `bytea`."""
    if not foto_base64:
        return None
    try:
        return base64.b64decode(foto_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(
            status_code=422, detail="foto_base64 no es base64 válido"
        ) from exc


@router.post("", response_model=UsuarioOut, status_code=201)
async def crear_usuario(
    payload: UsuarioCreate,
    conn: AsyncConnection = Depends(get_usuarios),
) -> UsuarioOut:
    """Crea un usuario y su perfil en UNA SOLA TRANSACCIÓN sobre `db_usuarios`.

    Concepto de sharding vertical: aunque las columnas estén repartidas en dos
    tablas (caliente/pesada), AMBAS viven en la misma base, por lo que una única
    transacción local las mantiene consistentes (sin 2PC ni coordinación
    distribuida). Devuelve 409 si el email ya existe.
    """
    foto = _decode_foto(payload.foto_base64)
    preferencias = (
        Jsonb(payload.preferencias) if payload.preferencias is not None else None
    )

    try:
        async with conn.cursor(row_factory=dict_row) as cur:
            # 1) Columnas calientes -> `usuarios`.
            await cur.execute(
                "INSERT INTO usuarios (nombre, email) VALUES (%s, %s) "
                "RETURNING id, nombre, email, creado_en",
                (payload.nombre, payload.email),
            )
            usuario = await cur.fetchone()
            # 2) Columnas pesadas -> `usuarios_perfil`, reutilizando el id generado.
            await cur.execute(
                "INSERT INTO usuarios_perfil (usuario_id, biografia, foto, preferencias) "
                "VALUES (%s, %s, %s, %s)",
                (usuario["id"], payload.biografia, foto, preferencias),
            )
        # Commit explícito: ambas inserciones se confirman juntas.
        await conn.commit()
    except UniqueViolation:
        await conn.rollback()
        raise HTTPException(
            status_code=409, detail=f"El email '{payload.email}' ya existe"
        ) from None

    return UsuarioOut(**usuario)


@router.get("", response_model=list[UsuarioOut])
async def listar_usuarios(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn: AsyncConnection = Depends(get_usuarios),
) -> list[UsuarioOut]:
    """Lista paginada de usuarios leyendo SOLO las columnas calientes.

    Concepto de sharding vertical (por columnas): al no referenciar
    `usuarios_perfil`, la consulta no lee las filas pesadas (biografia, foto,
    preferencias) y por tanto no paga su costo de I/O.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, nombre, email, creado_en FROM usuarios "
            "ORDER BY id LIMIT %s OFFSET %s",
            (limit, offset),
        )
        rows = await cur.fetchall()
    return [UsuarioOut(**row) for row in rows]


@router.get("/{usuario_id}", response_model=UsuarioOut)
async def obtener_usuario(
    usuario_id: int,
    conn: AsyncConnection = Depends(get_usuarios),
) -> UsuarioOut:
    """Devuelve un usuario leyendo SOLO `usuarios` (no toca `usuarios_perfil`).

    Concepto de sharding vertical (por columnas): esta consulta es deliberadamente
    "ligera"; el perfil pesado se obtiene en `/usuarios/{id}/perfil`. 404 si no existe.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, nombre, email, creado_en FROM usuarios WHERE id = %s",
            (usuario_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return UsuarioOut(**row)


@router.get("/{usuario_id}/perfil", response_model=UsuarioPerfilOut)
async def obtener_perfil(
    usuario_id: int,
    conn: AsyncConnection = Depends(get_usuarios),
) -> UsuarioPerfilOut:
    """Devuelve usuario + perfil mediante JOIN a las columnas pesadas.

    Concepto de sharding vertical (por columnas): aquí SÍ se reúnen ambas tablas
    (caliente + pesada). El JOIN es local a `db_usuarios`; la foto `bytea` se
    devuelve codificada en base64 para poder transportarla en JSON. 404 si no existe.
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT u.id, u.nombre, u.email, u.creado_en, "
            "       p.biografia, p.foto, p.preferencias "
            "FROM usuarios u "
            "LEFT JOIN usuarios_perfil p ON p.usuario_id = u.id "
            "WHERE u.id = %s",
            (usuario_id,),
        )
        row = await cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    foto = row.pop("foto")
    row["foto_base64"] = base64.b64encode(foto).decode("ascii") if foto else None
    return UsuarioPerfilOut(**row)

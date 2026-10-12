#!/usr/bin/env python3
"""scripts/seed.py — Genera datos de prueba para ShardShop.

Ilustra el SHARDING HORIZONTAL desde la carga de datos:

- Usuarios/perfiles -> `db_usuarios` (sharding VERTICAL). Se insertan por lotes
  con `executemany`; el perfil lleva biografía (~1 KB), foto aleatoria de 30-60 KB
  (bytea) y preferencias (jsonb).
- Productos -> reference table del cluster Citus (se replica en cada worker).
- Pedidos y detalle_pedidos -> tablas DISTRIBUIDAS por `cliente_id`, cargadas con
  `COPY` por lotes contra el coordinator. Citus reparte las filas entre los shards
  según `cliente_id`; el detalle usa el MISMO `cliente_id` (co-localizado).

Los ids se asignan de forma explícita (1..N) para poder enlazar pedido/detalle sin
depender de `RETURNING` durante el COPY; como las secuencias de Citus tienen rangos
disjuntos y muy altos por worker, no hay colisión con los ids sembrados.

Uso:
    python scripts/seed.py --reset --usuarios 1000 --pedidos 100000 --productos 50
"""

from __future__ import annotations

import argparse
import os
import random
import string
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

RAIZ = Path(__file__).resolve().parent.parent
ESTADOS = ["pendiente", "pagado", "enviado", "entregado"]
TEMAS = ["oscuro", "claro", "auto"]
IDIOMAS = ["es", "en", "pt"]


# --- Configuración (lee `.env` y variables de entorno) ----------------------


def cargar_dotenv(ruta: Path) -> dict[str, str]:
    """Lee un `.env` sencillo (KEY=VALUE) sin dependencias externas."""
    datos: dict[str, str] = {}
    if ruta.is_file():
        for linea in ruta.read_text(encoding="utf-8").splitlines():
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            clave, valor = linea.split("=", 1)
            datos[clave.strip()] = valor.strip()
    return datos


def resolver(env: dict[str, str], *nombres: str, default: str | None = None) -> str | None:
    """Devuelve el primer valor presente en el entorno o en `.env`."""
    for nombre in nombres:
        valor = os.getenv(nombre) or env.get(nombre)
        if valor:
            return valor
    return default


def parametros_conexion(env: dict[str, str]) -> tuple[dict, dict]:
    """Construye los parámetros de conexión de ambos dominios."""
    usuario = resolver(env, "POSTGRES_USER", "CITUS_USER", default="postgres")
    clave = resolver(env, "POSTGRES_PASSWORD", "CITUS_PASSWORD", default="postgres")

    citus = dict(
        host=resolver(env, "CITUS_HOST", default="localhost"),
        port=int(resolver(env, "CITUS_PORT", "COORDINATOR_PORT", default="5432")),
        user=usuario,
        password=clave,
        dbname=resolver(env, "CITUS_DB", default="postgres"),
    )
    usuarios = dict(
        host=resolver(env, "USUARIOS_HOST", default="localhost"),
        port=int(resolver(env, "USUARIOS_PORT", "DB_USUARIOS_PORT", default="5433")),
        user=usuario,
        password=clave,
        dbname=resolver(env, "USUARIOS_DB", default="postgres"),
    )
    return citus, usuarios


# --- Utilidades de progreso --------------------------------------------------


def barra(etiqueta: str, actual: int, total: int) -> None:
    """Barra de progreso simple en una línea."""
    pct = int(actual * 100 / total) if total else 100
    fin = "\n" if actual >= total else ""
    sys.stdout.write(f"\r  {etiqueta}: {pct:3d}% ({actual}/{total})" + fin)
    sys.stdout.flush()


# --- Reset -------------------------------------------------------------------


def reiniciar(cur_citus, cur_usuarios) -> None:
    """Limpia todo y reinicia las secuencias (idempotente).

    `TRUNCATE ... RESTART IDENTITY CASCADE` está soportado por Citus y propaga la
    limpieza a todos los shards; el CASCADE limpia el detalle por su FK.
    """
    # Los TRUNCATE son DDL multi-shard: Citus exige modo secuencial cuando hay
    # varias tablas distribuidas en la misma transacción. Se restaura el modo
    # paralelo (por defecto) para no penalizar el COPY posterior.
    cur_citus.execute("SET LOCAL citus.multi_shard_modify_mode TO 'sequential'")
    cur_citus.execute("TRUNCATE pedidos, detalle_pedidos RESTART IDENTITY CASCADE")
    cur_citus.execute("TRUNCATE productos RESTART IDENTITY CASCADE")
    cur_citus.execute("SET LOCAL citus.multi_shard_modify_mode TO 'parallel'")
    cur_usuarios.execute("TRUNCATE usuarios RESTART IDENTITY CASCADE")
    print("  reset: tablas truncadas y secuencias reiniciadas.")


# --- Usuarios (sharding vertical) -------------------------------------------


def sembrar_usuarios(cur, n: int) -> None:
    """Inserta usuarios y perfiles por lotes (`executemany`)."""
    ahora = datetime.now(timezone.utc)
    usuarios: list[tuple] = []
    perfiles: list[tuple] = []
    for i in range(1, n + 1):
        usuarios.append((i, f"Usuario {i}", f"usuario{i}@mail.com", ahora))
        biografia = "".join(random.choices(string.ascii_letters + " ", k=1024))
        foto = random.randbytes(random.randint(30 * 1024, 60 * 1024))
        preferencias = Jsonb(
            {
                "tema": random.choice(TEMAS),
                "idioma": random.choice(IDIOMAS),
                "notificaciones": random.choice([True, False]),
            }
        )
        perfiles.append((i, biografia, foto, preferencias))

    cur.executemany(
        "INSERT INTO usuarios (id, nombre, email, creado_en) VALUES (%s, %s, %s, %s)",
        usuarios,
    )
    cur.executemany(
        "INSERT INTO usuarios_perfil "
        "(usuario_id, biografia, foto, preferencias) VALUES (%s, %s, %s, %s)",
        perfiles,
    )
    barra("usuarios", n, n)


# --- Productos (reference table) --------------------------------------------


def sembrar_productos(cur, m: int) -> list[tuple[int, Decimal]]:
    """Inserta productos y devuelve [(id, precio)] para calcular totales."""
    filas = []
    resultado: list[tuple[int, Decimal]] = []
    for i in range(1, m + 1):
        precio = Decimal(str(round(random.uniform(5, 500), 2)))
        stock = random.randint(0, 200)
        filas.append((i, f"Producto {i}", precio, stock))
        resultado.append((i, precio))
    cur.executemany(
        "INSERT INTO productos (id, nombre, precio, stock) VALUES (%s, %s, %s, %s)",
        filas,
    )
    barra("productos", m, m)
    return resultado


# --- Pedidos y detalle (sharding horizontal, COPY por lotes) ----------------


def generar_pedido(
    pid: int,
    n_usuarios: int,
    productos_ids: list[int],
    precios: dict[int, Decimal],
    base: datetime,
) -> tuple[int, datetime, str, list[tuple[int, int]], Decimal]:
    """Genera un pedido determinista a partir de `pid` (semilla reproducible).

    Se usa un RNG sembrado con `pid` para poder recorrer los pedidos DOS veces
    (primero pedidos, luego detalle) obteniendo exactamente los mismos items.
    """
    rng = random.Random(pid)
    cliente_id = rng.randint(1, n_usuarios)
    fecha = base - timedelta(days=rng.randint(0, 365), seconds=rng.randint(0, 86400))
    estado = rng.choice(ESTADOS)
    items: list[tuple[int, int]] = []
    for _ in range(rng.randint(1, 4)):
        items.append((rng.choice(productos_ids), rng.randint(1, 5)))
    total = sum((precios[prod] * cant for prod, cant in items), Decimal("0"))
    return cliente_id, fecha, estado, items, total


def sembrar_pedidos(
    cur,
    p: int,
    n_usuarios: int,
    productos: list[tuple[int, Decimal]],
) -> int:
    """Carga pedidos y detalle con COPY; devuelve el número de líneas de detalle."""
    productos_ids = [pid for pid, _ in productos]
    precios = dict(productos)
    base = datetime.now(timezone.utc)
    paso = max(1, p // 10)

    # Paso 1: pedidos (COPY con ids explícitos).
    with cur.copy("COPY pedidos (id, cliente_id, fecha, estado, total) FROM STDIN") as copy:
        for pid in range(1, p + 1):
            cliente_id, fecha, estado, items, total = generar_pedido(
                pid, n_usuarios, productos_ids, precios, base
            )
            copy.write_row((pid, cliente_id, fecha, estado, total))
            if pid % paso == 0:
                barra("pedidos", pid, p)
    barra("pedidos", p, p)

    # Paso 2: detalle_pedidos, con el MISMO cliente_id (co-localizado con pedidos).
    detalle_id = 1
    with cur.copy(
        "COPY detalle_pedidos "
        "(id, cliente_id, pedido_id, producto_id, cantidad, precio_unitario) FROM STDIN"
    ) as copy:
        for pid in range(1, p + 1):
            cliente_id, _, _, items, _ = generar_pedido(
                pid, n_usuarios, productos_ids, precios, base
            )
            for prod, cant in items:
                copy.write_row((detalle_id, cliente_id, pid, prod, cant, precios[prod]))
                detalle_id += 1
            if pid % paso == 0:
                barra("detalle_pedidos", pid, p)
    barra("detalle_pedidos", p, p)
    return detalle_id - 1


# --- Sincronización de secuencias -------------------------------------------


def sincronizar_secuencias(cur_citus, cur_usuarios) -> None:
    """Ajusta cada secuencia serial al `max(id)` de su tabla.

    El COPY/INSERT con ids explícitos NO avanza las secuencias. Como las
    secuencias seriales (`usuarios`, `productos`, `pedidos`, `detalle_pedidos`)
    empiezan en 1, sin este ajuste el siguiente `nextval` de la app reutilizaría
    ids ya sembrados y provocaría `UniqueViolation` (el PK de las distribuidas es
    compuesto `(cliente_id, id)`, así que además fallaría de forma intermitente).

    Para las tablas distribuidas se calcula el `max(id)` con una consulta aparte (que
    Citus sí agrega en todos los shards) y se pasa el valor literal a `setval`;
    meter el `max(id)` como subconsulta dentro del `setval` no se agrega global y
    devolvería NULL (por eso `productos`, reference, quedaba en 1).
    """
    for tabla in ("pedidos", "detalle_pedidos", "productos"):
        cur_citus.execute(f"SELECT COALESCE(max(id), 1) FROM {tabla}")
        max_id = cur_citus.fetchone()[0]
        cur_citus.execute(
            "SELECT setval(pg_get_serial_sequence(%s, 'id'), %s)", (tabla, max_id)
        )
    cur_usuarios.execute(
        "SELECT setval(pg_get_serial_sequence('usuarios', 'id'), "
        "(SELECT COALESCE(max(id), 1) FROM usuarios))"
    )
    print("  secuencias sincronizadas con max(id).")


# --- main --------------------------------------------------------------------


def parsear_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Siembra datos de prueba en ShardShop.")
    parser.add_argument("--usuarios", type=int, default=1000, help="nº de usuarios (1000)")
    parser.add_argument("--pedidos", type=int, default=100000, help="nº de pedidos (100000)")
    parser.add_argument("--productos", type=int, default=50, help="nº de productos (50)")
    parser.add_argument(
        "--reset", action="store_true", help="trunca las tablas y reinicia secuencias"
    )
    return parser.parse_args()


def main() -> int:
    args = parsear_args()
    env = cargar_dotenv(RAIZ / ".env")
    citus, usuarios = parametros_conexion(env)

    print("ShardShop · seed")
    print(f"  citus:    {citus['host']}:{citus['port']}/{citus['dbname']}")
    print(f"  usuarios: {usuarios['host']}:{usuarios['port']}/{usuarios['dbname']}")
    print(
        f"  objetivo: usuarios={args.usuarios} pedidos={args.pedidos} "
        f"productos={args.productos} reset={args.reset}"
    )

    inicio = time.perf_counter()
    try:
        with psycopg.connect(**citus) as conn_citus, psycopg.connect(**usuarios) as conn_usuarios:
            with conn_citus.cursor() as cur_citus, conn_usuarios.cursor() as cur_usuarios:
                if args.reset:
                    reiniciar(cur_citus, cur_usuarios)

                if args.usuarios > 0:
                    sembrar_usuarios(cur_usuarios, args.usuarios)

                productos = []
                if args.productos > 0:
                    productos = sembrar_productos(cur_citus, args.productos)

                if args.pedidos > 0:
                    if not productos:
                        raise RuntimeError(
                            "Se necesitan productos para crear pedidos "
                            "(usa --productos > 0 o siembra productos antes)."
                        )
                    n_detalle = sembrar_pedidos(
                        cur_citus, args.pedidos, args.usuarios, productos
                    )
                    print(f"  detalle_pedidos: {n_detalle} líneas")

                # Deja las secuencias listas para los INSERT de la app.
                sincronizar_secuencias(cur_citus, cur_usuarios)
    except psycopg.Error as exc:
        print(f"\n[ERROR] {exc}", file=sys.stderr)
        return 1

    transcurrido = time.perf_counter() - inicio
    print(f"Listo en {transcurrido:.2f}s.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

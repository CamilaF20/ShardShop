#!/usr/bin/env python3
"""scripts/validate_horizontal.py — Validación del SHARDING HORIZONTAL (Fase 9).

Ejecuta una batería de comprobaciones sobre el cluster Citus y escribe el informe
en `docs/evidencias/horizontal.md`. Cada prueba imprime PASS o FAIL y el script
devuelve código de salida 1 si alguna falla.

Comprobaciones:
  1. 2 workers activos.
  2. La suma de filas por shard = total de `pedidos`, repartido entre los workers.
  3. EXPLAIN de una consulta por `cliente_id` -> Task Count: 1.
  4. EXPLAIN de una agregación global -> Task Count = nº de shards.
  5. Co-location: `pedidos` y `detalle_pedidos` comparten colocationid y su JOIN
     por `cliente_id` es de un solo task.
  6. Tiempos: consulta por cliente vs agregación global.

Sin ORM: SQL explícito con psycopg 3. Lee la conexión de `.env`/entorno como el
resto del proyecto.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

RAIZ = Path(__file__).resolve().parent.parent
INFORME = RAIZ / "docs" / "evidencias" / "horizontal.md"

# `EXPLAIN` de una consulta distribuida incluye una línea "Task Count: N".
_TASK_COUNT_RE = re.compile(r"Task Count:\s*(\d+)")


# --- Configuración (misma lógica que scripts/seed.py) ------------------------


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


def conexion_citus(env: dict[str, str]) -> dict[str, object]:
    """Parámetros de conexión al coordinator de Citus."""
    return dict(
        host=resolver(env, "CITUS_HOST", default="localhost"),
        port=int(resolver(env, "CITUS_PORT", "COORDINATOR_PORT", default="5432")),
        user=resolver(env, "POSTGRES_USER", "CITUS_USER", default="postgres"),
        password=resolver(env, "POSTGRES_PASSWORD", "CITUS_PASSWORD", default="postgres"),
        dbname=resolver(env, "CITUS_DB", default="postgres"),
    )


# --- Utilidades --------------------------------------------------------------


def task_count(plan: str) -> int | None:
    """Extrae el `Task Count` del plan textual de un EXPLAIN."""
    coincidencia = _TASK_COUNT_RE.search(plan)
    return int(coincidencia.group(1)) if coincidencia else None


def explain(cur, sql: str) -> str:
    """Devuelve el plan textual de `EXPLAIN sql`."""
    cur.execute(f"EXPLAIN {sql}")
    return "\n".join(fila[0] for fila in cur.fetchall())


def parsear_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validación del sharding horizontal.")
    parser.add_argument("--cliente-id", type=int, default=42, help="cliente para EXPLAIN/tiempos")
    parser.add_argument("--iteraciones", type=int, default=50, help="iteraciones de la prueba de tiempos")
    return parser.parse_args()


def main() -> int:
    args = parsear_args()
    env = cargar_dotenv(RAIZ / ".env")
    params = conexion_citus(env)
    print("ShardShop · validación HORIZONTAL")
    print(f"  citus: {params['host']}:{params['port']}/{params['dbname']}\n")

    pruebas: list[tuple[str, bool, str]] = []

    def registrar(nombre: str, ok: bool, detalle: str = "") -> None:
        print(f"[{'PASS' if ok else 'FAIL'}] {nombre}" + (f" — {detalle}" if detalle else ""))
        pruebas.append((nombre, ok, detalle))

    try:
        with psycopg.connect(**params) as conn:
            cur = conn.cursor()

            # 1) Workers activos.
            cur.execute("SELECT count(*) FROM citus_get_active_worker_nodes()")
            n_workers = cur.fetchone()[0]
            registrar("1. Dos workers activos", n_workers == 2, f"{n_workers} workers")

            # 2) Suma de filas por shard = total, repartido en ambos workers.
            cur.execute("SELECT count(*) FROM pedidos")
            total = cur.fetchone()[0]
            cur.execute(
                "SELECT cs.nodename, sum(r.result::bigint) AS filas "
                "FROM run_command_on_shards('pedidos', $$SELECT count(*) FROM %s$$) r "
                "JOIN citus_shards cs "
                "  ON cs.shardid = r.shardid AND cs.table_name = 'pedidos'::regclass "
                "WHERE r.success GROUP BY cs.nodename ORDER BY cs.nodename"
            )
            por_nodo = cur.fetchall()
            suma = sum(fila[1] for fila in por_nodo)
            ambos = len(por_nodo) == 2 and all(fila[1] > 0 for fila in por_nodo)
            detalle2 = "suma shards=" + str(suma) + " vs total=" + str(total) + "; " + ", ".join(
                f"{nodo}={filas} ({100 * filas / total:.1f}%)" for nodo, filas in por_nodo
            ) if total else "sin pedidos"
            registrar("2. Reparto por shard == total y en 2 workers", suma == total and ambos, detalle2)

            # 3) EXPLAIN enrutado por cliente_id -> Task Count 1.
            plan_ruta = explain(cur, f"SELECT * FROM pedidos WHERE cliente_id = {args.cliente_id}")
            tc_ruta = task_count(plan_ruta)
            registrar("3. EXPLAIN enrutado (Task Count 1)", tc_ruta == 1, f"Task Count={tc_ruta}")

            # 4) EXPLAIN agregación global -> Task Count = nº de shards.
            cur.execute("SELECT count(*) FROM citus_shards WHERE table_name = 'pedidos'::regclass")
            n_shards = cur.fetchone()[0]
            plan_global = explain(cur, "SELECT count(*), sum(total) FROM pedidos")
            tc_global = task_count(plan_global)
            registrar(
                "4. EXPLAIN global (Task Count = nº de shards)",
                tc_global == n_shards,
                f"Task Count={tc_global}, shards={n_shards}",
            )

            # 5) Co-location pedidos/detalle_pedidos + JOIN de un solo task.
            cur.execute(
                "SELECT logicalrelid::text, colocationid FROM pg_dist_partition "
                "WHERE logicalrelid IN ('pedidos'::regclass, 'detalle_pedidos'::regclass)"
            )
            colocs = dict(cur.fetchall())
            mismo = (
                len(colocs) == 2
                and colocs.get("pedidos") is not None
                and colocs.get("pedidos") == colocs.get("detalle_pedidos")
            )
            plan_join = explain(
                cur,
                "SELECT p.id FROM pedidos p "
                "JOIN detalle_pedidos d "
                "  ON d.cliente_id = p.cliente_id AND d.pedido_id = p.id "
                f"WHERE p.cliente_id = {args.cliente_id}",
            )
            tc_join = task_count(plan_join)
            registrar(
                "5. Co-location + JOIN de un solo task",
                mismo and tc_join == 1,
                f"colocationid={colocs.get('pedidos')}, Task Count JOIN={tc_join}",
            )

            # 6) Tiempos: consulta por cliente vs agregación global.
            n = args.iteraciones
            inicio = time.perf_counter()
            for _ in range(n):
                cur.execute("SELECT * FROM pedidos WHERE cliente_id = %s", (args.cliente_id,))
                cur.fetchall()
            t_enrutada = (time.perf_counter() - inicio) * 1000

            inicio = time.perf_counter()
            for _ in range(n):
                cur.execute("SELECT count(*), sum(total) FROM pedidos")
                cur.fetchall()
            t_global = (time.perf_counter() - inicio) * 1000

            registrar(
                "6. Tiempos: enrutada vs global",
                True,
                f"{n} iter · enrutada {t_enrutada:.1f} ms vs global {t_global:.1f} ms",
            )
    except psycopg.Error as exc:
        registrar("Conexión a Citus", False, str(exc))
        t_enrutada = t_global = 0.0

    _escribir_informe(env, params, args, pruebas, t_enrutada, t_global)

    fallos = sum(1 for _, ok, _ in pruebas if not ok)
    print(f"\nResultado: {len(pruebas) - fallos}/{len(pruebas)} PASS · informe en {INFORME}")
    return 1 if fallos else 0


def _escribir_informe(env, params, args, pruebas, t_enrutada, t_global) -> None:
    """Escribe el informe en Markdown con marca de tiempo."""
    INFORME.parent.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    fallos = sum(1 for _, ok, _ in pruebas if not ok)
    lineas = [
        "# Evidencia — sharding horizontal (Citus)",
        "",
        f"- **Fecha:** {ahora}",
        f"- **Cluster:** `{params['host']}:{params['port']}/{params['dbname']}`",
        f"- **Cliente de prueba:** {args.cliente_id} · **Iteraciones de tiempo:** {args.iteraciones}",
        f"- **Resultado:** {'PASS' if not fallos else 'FAIL'} "
        f"({len(pruebas) - fallos}/{len(pruebas)} pruebas)",
        "",
        "| # | Prueba | Resultado | Detalle |",
        "|---|---|---|---|",
    ]
    for nombre, ok, detalle in pruebas:
        lineas.append(f"| {nombre.split('.')[0]} | {nombre} | {'PASS' if ok else 'FAIL'} | {detalle} |")
    lineas += [
        "",
        "## Tiempos (prueba 6)",
        "",
        f"- Consulta enrutada por `cliente_id = {args.cliente_id}`: **{t_enrutada:.2f} ms** "
        f"({args.iteraciones} iteraciones)",
        f"- Agregación global: **{t_global:.2f} ms** ({args.iteraciones} iteraciones)",
        "",
        "> La consulta enrutada toca un único shard (Task Count 1); la agregación global "
        "lanza una tarea por shard (Task Count = nº de shards).",
        "",
    ]
    INFORME.write_text("\n".join(lineas), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())

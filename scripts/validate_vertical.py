#!/usr/bin/env python3
"""scripts/validate_vertical.py — Validación del SHARDING VERTICAL (Fase 9).

Comprueba que el dominio de usuarios está separado y que las columnas pesadas
viven en su propia tabla, y escribe el informe en `docs/evidencias/vertical.md`.
Cada prueba imprime PASS o FAIL y el script devuelve 1 si alguna falla.

Comprobaciones:
  1. `usuarios` y `usuarios_perfil` existen en `db_usuarios` y NO en el cluster Citus
     (sharding vertical por dominio).
  2. `pg_total_relation_size('usuarios')` << `pg_total_relation_size('usuarios_perfil')`
     (sharding vertical por columnas).
  3. Tiempos: N lecturas de columnas calientes vs N lecturas con columnas pesadas.

Sin ORM: SQL explícito con psycopg 3.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

RAIZ = Path(__file__).resolve().parent.parent
INFORME = RAIZ / "docs" / "evidencias" / "vertical.md"


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


def conexion(dbname_env: str, host_env: str, port_env: tuple[str, ...], default_port: int, env):
    """Construye los parámetros de conexión de un dominio."""
    return dict(
        host=resolver(env, host_env, default="localhost"),
        port=int(resolver(env, *port_env, default=str(default_port))),
        user=resolver(env, "POSTGRES_USER", default="postgres"),
        password=resolver(env, "POSTGRES_PASSWORD", default="postgres"),
        dbname=resolver(env, dbname_env, default="postgres"),
    )


def parsear_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validación del sharding vertical.")
    parser.add_argument("--iteraciones", type=int, default=200, help="iteraciones de la prueba de tiempos")
    return parser.parse_args()


def main() -> int:
    args = parsear_args()
    env = cargar_dotenv(RAIZ / ".env")
    params_usu = conexion("USUARIOS_DB", "USUARIOS_HOST", ("USUARIOS_PORT", "DB_USUARIOS_PORT"), 5433, env)
    params_cit = conexion("CITUS_DB", "CITUS_HOST", ("CITUS_PORT", "COORDINATOR_PORT"), 5432, env)
    print("ShardShop · validación VERTICAL")
    print(f"  usuarios: {params_usu['host']}:{params_usu['port']}/{params_usu['dbname']}")
    print(f"  citus:    {params_cit['host']}:{params_cit['port']}/{params_cit['dbname']}\n")

    pruebas: list[tuple[str, bool, str]] = []

    def registrar(nombre: str, ok: bool, detalle: str = "") -> None:
        print(f"[{'PASS' if ok else 'FAIL'}] {nombre}" + (f" — {detalle}" if detalle else ""))
        pruebas.append((nombre, ok, detalle))

    t_cal = t_pes = 0.0
    factor = 0.0
    try:
        with psycopg.connect(**params_usu) as conn_u, psycopg.connect(**params_cit) as conn_c:
            cur_u = conn_u.cursor()
            cur_c = conn_c.cursor()

            # 1) Existen en db_usuarios y NO en el cluster Citus (sharding por dominio).
            cur_u.execute(
                "SELECT to_regclass('public.usuarios') IS NOT NULL, "
                "       to_regclass('public.usuarios_perfil') IS NOT NULL"
            )
            en_usu = cur_u.fetchone()
            cur_c.execute(
                "SELECT to_regclass('public.usuarios') IS NOT NULL, "
                "       to_regclass('public.usuarios_perfil') IS NOT NULL"
            )
            en_cit = cur_c.fetchone()
            ok1 = all(en_usu) and not any(en_cit)
            registrar(
                "1. Tablas verticales en db_usuarios y ausentes en Citus",
                ok1,
                f"db_usuarios={en_usu}, citus={en_cit}",
            )

            # 2) Columnas pesadas ocupan mucho más que las calientes.
            cur_u.execute(
                "SELECT pg_total_relation_size('usuarios'), "
                "       pg_total_relation_size('usuarios_perfil')"
            )
            size_cal, size_pes = cur_u.fetchone()
            ok2 = size_pes > size_cal
            registrar(
                "2. usuarios_perfil (pesado) >> usuarios (caliente)",
                ok2,
                f"usuarios={size_cal} B, usuarios_perfil={size_pes} B "
                f"(x{size_pes / size_cal:.1f})" if size_cal else "tablas vacías",
            )

            # 3) Tiempos: N lecturas calientes vs N con JOIN a columnas pesadas.
            cur_u.execute("SELECT id FROM usuarios ORDER BY id LIMIT 1")
            fila = cur_u.fetchone()
            if fila is None:
                registrar("3. Tiempos caliente vs pesado", False, "no hay usuarios; ejecuta scripts/seed.py")
            else:
                usuario_id = fila[0]
                sql_cal = "SELECT id, nombre, email, creado_en FROM usuarios WHERE id = %s"
                sql_pes = (
                    "SELECT u.id, u.nombre, u.email, u.creado_en, "
                    "       p.biografia, p.foto, p.preferencias "
                    "FROM usuarios u JOIN usuarios_perfil p ON p.usuario_id = u.id "
                    "WHERE u.id = %s"
                )
                n = args.iteraciones
                inicio = time.perf_counter()
                for _ in range(n):
                    cur_u.execute(sql_cal, (usuario_id,))
                    cur_u.fetchall()
                t_cal = (time.perf_counter() - inicio) * 1000

                inicio = time.perf_counter()
                for _ in range(n):
                    cur_u.execute(sql_pes, (usuario_id,))
                    cur_u.fetchall()
                t_pes = (time.perf_counter() - inicio) * 1000

                factor = (t_pes / t_cal) if t_cal > 0 else 0.0
                registrar(
                    "3. Tiempos caliente vs pesado",
                    factor > 1.0,
                    f"{n} iter · caliente {t_cal:.1f} ms vs pesado {t_pes:.1f} ms (factor {factor:.2f}x)",
                )
    except psycopg.Error as exc:
        registrar("Conexión a las bases", False, str(exc))

    _escribir_informe(params_usu, params_cit, args, pruebas, t_cal, t_pes, factor)

    fallos = sum(1 for _, ok, _ in pruebas if not ok)
    print(f"\nResultado: {len(pruebas) - fallos}/{len(pruebas)} PASS · informe en {INFORME}")
    return 1 if fallos else 0


def _escribir_informe(params_usu, params_cit, args, pruebas, t_cal, t_pes, factor) -> None:
    """Escribe el informe en Markdown con marca de tiempo."""
    INFORME.parent.mkdir(parents=True, exist_ok=True)
    ahora = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    fallos = sum(1 for _, ok, _ in pruebas if not ok)
    lineas = [
        "# Evidencia — sharding vertical (usuarios)",
        "",
        f"- **Fecha:** {ahora}",
        f"- **db_usuarios:** `{params_usu['host']}:{params_usu['port']}/{params_usu['dbname']}`",
        f"- **Cluster Citus:** `{params_cit['host']}:{params_cit['port']}/{params_cit['dbname']}`",
        f"- **Iteraciones de tiempo:** {args.iteraciones}",
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
        "## Tiempos (prueba 3)",
        "",
        f"- Lecturas solo de columnas calientes: **{t_cal:.2f} ms** ({args.iteraciones} iteraciones)",
        f"- Lecturas con columnas pesadas (JOIN): **{t_pes:.2f} ms** ({args.iteraciones} iteraciones)",
        f"- Factor de mejora (pesado/caliente): **{factor:.2f}x**",
        "",
        "> Separar las columnas pesadas (biografía, foto, preferencias) del acceso "
        "caliente reduce el I/O en el caso de uso más frecuente.",
        "",
    ]
    INFORME.write_text("\n".join(lineas), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())

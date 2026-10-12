# Evidencia — sharding vertical (usuarios)

- **Fecha:** 2026-10-12 14:42:08 UTC
- **db_usuarios:** `db_usuarios:5432/postgres`
- **Cluster Citus:** `coordinator:5432/postgres`
- **Iteraciones de tiempo:** 200
- **Resultado:** PASS (3/3 pruebas)

| # | Prueba | Resultado | Detalle |
|---|---|---|---|
| 1 | 1. Tablas verticales en db_usuarios y ausentes en Citus | PASS | db_usuarios=(True, True), citus=(False, False) |
| 2 | 2. usuarios_perfil (pesado) >> usuarios (caliente) | PASS | usuarios=229376 B, usuarios_perfil=49152000 B (x214.3) |
| 3 | 3. Tiempos caliente vs pesado | PASS | 200 iter · caliente 19.3 ms vs pesado 114.7 ms (factor 5.94x) |

## Tiempos (prueba 3)

- Lecturas solo de columnas calientes: **19.32 ms** (200 iteraciones)
- Lecturas con columnas pesadas (JOIN): **114.71 ms** (200 iteraciones)
- Factor de mejora (pesado/caliente): **5.94x**

> Separar las columnas pesadas (biografía, foto, preferencias) del acceso caliente reduce el I/O en el caso de uso más frecuente.

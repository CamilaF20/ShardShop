# Evidencia — sharding horizontal (Citus)

- **Fecha:** 2026-10-12 14:42:06 UTC
- **Cluster:** `coordinator:5432/postgres`
- **Cliente de prueba:** 42 · **Iteraciones de tiempo:** 50
- **Resultado:** PASS (6/6 pruebas)

| # | Prueba | Resultado | Detalle |
|---|---|---|---|
| 1 | 1. Dos workers activos | PASS | 2 workers |
| 2 | 2. Reparto por shard == total y en 2 workers | PASS | suma shards=100000 vs total=100000; worker1=48954 (49.0%), worker2=51046 (51.0%) |
| 3 | 3. EXPLAIN enrutado (Task Count 1) | PASS | Task Count=1 |
| 4 | 4. EXPLAIN global (Task Count = nº de shards) | PASS | Task Count=32, shards=32 |
| 5 | 5. Co-location + JOIN de un solo task | PASS | colocationid=2, Task Count JOIN=1 |
| 6 | 6. Tiempos: enrutada vs global | PASS | 50 iter · enrutada 44.5 ms vs global 475.0 ms |

## Tiempos (prueba 6)

- Consulta enrutada por `cliente_id = 42`: **44.52 ms** (50 iteraciones)
- Agregación global: **475.01 ms** (50 iteraciones)

> La consulta enrutada toca un único shard (Task Count 1); la agregación global lanza una tarea por shard (Task Count = nº de shards).

# Escalabilidad y tolerancia a fallos — ShardShop

Este documento acompaña a `scripts/demo_escalabilidad.sh` (Fase 10) y describe
**qué se espera ver** al escalar el cluster Citus en caliente y **qué limitaciones**
tiene el diseño actual.

## Cómo ejecutarlo

Desde la raíz del repositorio, con el stack base levantado:

```bash
docker compose up -d --build      # cluster base de 2 workers
python scripts/seed.py --reset --usuarios 1000 --pedidos 100000
./scripts/demo_escalabilidad.sh   # avanza con pausas (Enter)
```

El script pide confirmación (`read -p`) entre pasos para poder explicar cada uno.

## Qué hace el script, paso a paso

| Paso | Acción | Concepto de Citus |
|---|---|---|
| 0 | Comprueba el coordinator y lista workers activos | `citus_get_active_worker_nodes()` |
| 1 | Muestra shards y filas de `pedidos` por worker | `citus_shards` ⋈ `run_command_on_shards` |
| 2 | `docker compose --profile scale up -d worker3`, `citus_add_node`, `replicate_reference_tables`, `rebalance_table_shards(..., block_writes)` | escalado horizontal y rebalanceo |
| 3 | Vuelve a mostrar la distribución (ahora 3 workers) | reparto uniforme de shards |
| 4 | `docker stop` de un worker; prueba una consulta a su shard (falla) y otra a un shard sano (funciona); reinicia | tolerancia a fallos sin replicación |

## Resultado esperado

1. **Antes:** 32 shards de `pedidos` repartidos entre `worker1` y `worker2`
   (≈50% de filas cada uno).
2. **Después de añadir `worker3` y rebalancear:** los 32 shards se reparten entre
   los **3 workers** (≈1/3 cada uno en número de shards). Se puede comprobar con
   `GET /shards/distribucion` o con el SQL del script.
3. **Fallo de un worker:** al detener, por ejemplo, `worker2`:
   - una consulta enrutada a un `cliente_id` cuyo shard vive en ese worker
     **falla** (error de conexión al nodo);
   - una consulta a un `cliente_id` cuyo shard vive en otro worker **sigue
     funcionando**;
   - una **agregación global** (`SELECT count(*) FROM pedidos`) también falla,
     porque toca todos los shards.
4. **Tras reiniciar** el worker, sus shards vuelven a estar disponibles: la
   consulta que fallaba vuelve a responder.

## Limitaciones (importante)

- **Sin replicación de shards.** No se configuró `replication_factor` ni streaming
  replication entre workers, así que el fallo de un nodo deja **inaccesibles sus
  shards** (no hay failover automático). Esta es exactamente la conducta que la
  demo evidencia.
- **Cobertura parcial ante fallos.** Solo se degrada una parte del sistema: los
  shards de los nodos sanos siguen sirviendo. En producción se combinaría con
  réplicas de shard (`citus_set_shard_replication_factor`) o Patroni/replicación
  síncrona.
- **Rebalanceo con `block_writes`.** Se usa `shard_transfer_mode := 'block_writes'`
  para no exigir `wal_level=logical`. La contrapartida es que los shards en
  movimiento bloquean la escritura mientras se copian (para este volumen es
  cuestión de segundos).
- **El coordinator no almacena shards** (no se llamó a `citus_set_coordinator_host`),
  por lo que sigue siendo un punto único de entrada (aunque no guarda datos).
- **Estado persistente del nodo.** Al añadir `worker3`, queda registrado en
  `pg_dist_node` (en el volumen del coordinator). Si luego se levanta el stack
  **sin** el perfil `scale`, `worker3` no arrancará pero seguirá en la metadata, y
  las consultas a sus shards fallarán. Opciones:
  - arrancar siempre con `docker compose --profile scale up -d`; o
  - volver al cluster base quitando el nodo (ver cierre del script):
    ```sql
    SELECT citus_remove_node('worker3', 5432);
    ```
- **`hot shards`.** El reparto es por `cliente_id`; si unos pocos clientes
  concentran la mayoría de pedidos, sus shards quedan más cargados aunque el
  rebalanceo iguale el **número** de shards por nodo, no el volumen real.

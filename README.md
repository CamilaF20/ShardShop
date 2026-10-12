# ShardShop

Tienda en línea de demostración que ilustra **sharding horizontal y vertical**
sobre **PostgreSQL + Citus 12.1**. Todo corre en local con Docker Compose y se
puede observar desde un panel web.

- **Sharding horizontal:** `pedidos` y `detalle_pedidos` distribuidas por
  `cliente_id` (co-localizadas); `productos` como *reference table* replicada en
  cada worker.
- **Sharding vertical:** los usuarios viven en un PostgreSQL independiente
  (`db_usuarios`, dominio separado) y sus columnas pesadas se aíslan en
  `usuarios_perfil`.

> El diseño se explica en [`docs/arquitectura.md`](docs/arquitectura.md), la
> escalabilidad en [`docs/escalabilidad.md`](docs/escalabilidad.md) y las
> evidencias de las validaciones en [`docs/evidencias/`](docs/evidencias/).

## Arquitectura

```
                    ┌──────────────┐
   Navegador ─────► │  app FastAPI │ :8000   (panel + API)
                    └───┬──────┬───┘
        pool_citus      │      │   pool_usuarios
                ┌───────▼──┐ ┌─▼────────────┐
                │coordinator│ │ db_usuarios  │  PostgreSQL 16 (sin Citus)
                │  :5432    │ │   :5433      │  usuarios, usuarios_perfil
                └──┬─────┬──┘ └──────────────┘
              ┌────▼──┐ ┌▼──────┐   (worker3 opcional, perfil "scale")
              │worker1│ │worker2│   shards de pedidos / detalle_pedidos
              └───────┘ └───────┘   copia de productos (reference)
```

| Tabla | Ubicación | Tipo | Notas |
|---|---|---|---|
| `productos` | Cluster Citus | Reference table | copia completa en cada worker |
| `pedidos` | Cluster Citus | Horizontal | distribuida por `cliente_id` (32 shards) |
| `detalle_pedidos` | Cluster Citus | Horizontal | distribuida por `cliente_id`, co-localizada con `pedidos` |
| `usuarios` | `db_usuarios` | Vertical (dominio + columnas) | columnas ligeras/básicas |
| `usuarios_perfil` | `db_usuarios` | Vertical (columnas) | `biografia`, `foto` (bytea), `preferencias` (jsonb) |

## Requisitos

- Docker y Docker Compose v2 (`docker compose`).
- (Opcional) Python 3.11 en el host para ejecutar los scripts de siembra y
  validación fuera de Docker; también se pueden correr dentro de la imagen `app`.

## Puesta en marcha

```bash
# 1. Configuración (opcional: usa los defaults si no lo copias)
cp .env.example .env

# 2. Levanta el stack (coordinator, worker1, worker2, db_usuarios, app)
docker compose up -d --build

# 3. Comprueba el estado
docker compose ps                 # todos los servicios "healthy"/"running"
curl localhost:8000/health

# 4. Siembra datos de prueba (1.000 usuarios / 100.000 pedidos / 50 productos)
python scripts/seed.py --reset --usuarios 1000 --pedidos 100000 --productos 50

# 5. Abre el panel
open http://localhost:8000
```

La API queda en `http://localhost:8000` (Swagger en `/docs`), el coordinator de
Citus en `localhost:5432` y `db_usuarios` en `localhost:5433`.

### Siembra (`scripts/seed.py`)

Genera usuarios/perfiles en `db_usuarios`, productos en la reference table y
pedidos + detalle con `COPY` por lotes, repartidos por `cliente_id`. Al final
sincroniza las secuencias seriales con `max(id)` para que los INSERT de la app no
reutilicen ids.

```bash
python scripts/seed.py --reset            # limpia y resiembra (defaults)
python scripts/seed.py --usuarios 5000 --pedidos 500000
python scripts/seed.py --help
```

## Validación

```bash
python scripts/validate_horizontal.py   # 6/6 PASS (2 workers, reparto, Task Count 1 vs 32, co-location, tiempos)
python scripts/validate_vertical.py     # 3/3 PASS (dominio separado, tamaño, tiempos)
pytest -q                               # 5 passed (tests de integración con TestClient)
```

Los validadores escriben informes con marca de tiempo en
`docs/evidencias/horizontal.md` y `docs/evidencias/vertical.md`.

## Panel web

Un solo `app/static/index.html` (JS vanilla + Chart.js por CDN), con barra
superior de pestañas y estados de carga/error en cada una.

| Pestaña | Qué muestra |
|---|---|
| **Arquitectura** | mapa del sistema y estado de ambas bases + workers (`/health`, `/admin/nodos`) |
| **Distribución** | gráfico de barras de filas por worker, chips de los 32 shards y tabla de shards (`/shards/distribucion`) |
| **Enrutamiento** | diagrama `cliente_id → hash → shard → worker` y `EXPLAIN` enrutado vs global (Task Count 1 vs 32) |
| **Vertical** | esquema `usuarios` (datos básicos) vs `usuarios_perfil` (perfil) y gráfico de `/demo/vertical` |
| **Tienda** | crear usuario, crear pedido (muestra en qué shard cayó) y consultar pedidos de un cliente |
| **Estadísticas** | agregados globales (`/estadisticas`) y botón de rebalanceo (`/admin/rebalance`) |

## Tabla de endpoints

| Método | Ruta | Descripción | Concepto |
|---|---|---|---|
| GET | `/health` | estado de Citus, `db_usuarios` y workers | sharding vertical (2 bases) + horizontal (workers) |
| POST | `/usuarios` | crea usuario + perfil en una transacción (409 si el email existe) | vertical por columnas, transacción local |
| GET | `/usuarios` | lista paginada (`limit`, `offset`, solo columnas básicas) | vertical por columnas |
| GET | `/usuarios/{id}` | un usuario (sin tocar el perfil) | vertical por columnas |
| GET | `/usuarios/{id}/perfil` | usuario + JOIN con columnas pesadas | vertical por columnas |
| GET | `/demo/vertical?n=200` | tiempos básicas vs con perfil + factor de mejora | justifica aislar columnas pesadas |
| POST | `/productos` | crea producto en la reference table | reference table propagada a los workers |
| GET | `/productos` | catálogo completo | consulta de una reference table |
| POST | `/pedidos` | pedido + detalle en una transacción | co-localización ⇒ transacción local (sin 2PC) |
| GET | `/clientes/{cliente_id}/pedidos` | pedidos con detalle (JOIN) | poda a un shard + JOIN local |
| GET | `/estadisticas` | totales y top 5 de productos | agregación en todos los shards (Task Count 32) |
| GET | `/shards/distribucion` | filas por shard y por nodo | `run_command_on_shards` ⋈ `citus_shards` |
| GET | `/shards/ubicacion/{cliente_id}` | shard y worker del cliente | `get_shard_id_for_distribution_column` |
| GET | `/shards/explain?cliente_id=42` | plan enrutado vs distribuido | Task Count 1 vs 32 |
| GET | `/admin/nodos` | nodos en `pg_dist_node` | metadata del cluster |
| POST | `/admin/rebalance` | rebalancea shards | `rebalance_table_shards` (`block_writes`) |

## Estructura del repositorio

```
ShardShop/
├── docker-compose.yml      # coordinator + worker1/2 (+worker3 "scale") + db_usuarios + app
├── Dockerfile              # imagen del backend FastAPI
├── requirements.txt        # dependencias con versiones fijas
├── .env.example            # credenciales y puertos (copia a .env)
├── sql/                    # citus_schema.sql, usuarios_schema.sql
├── scripts/                # init_cluster.sh, seed.py, validate_*.py, demo_escalabilidad.sh
├── app/                    # main.py, config.py, db.py, schemas.py, routers/, static/index.html
├── tests/                  # test_api.py (pytest + TestClient)
└── docs/                   # arquitectura.md, escalabilidad.md, evidencias/
```

## Escalabilidad y tolerancia a fallos

`scripts/demo_escalabilidad.sh` (Fase 10) demuestra el escalado horizontal y el
comportamiento ante fallos, con pausas (`read -p`) entre pasos:

1. Distribución actual (2 workers).
2. `docker compose --profile scale up -d worker3`, `citus_add_node` y
   `rebalance_table_shards` (`block_writes`).
3. Nueva distribución entre **3 workers**.
4. `docker stop` de un worker: las consultas a sus shards fallan, las de otros
   workers siguen funcionando; al reiniciar, sus shards vuelven.

```bash
docker compose up -d                     # cluster base de 2 workers
./scripts/demo_escalabilidad.sh          # avanza con Enter
```

Detalles y limitaciones (sin replicación de shards, `block_writes`, coordinador
sin shards) en [`docs/escalabilidad.md`](docs/escalabilidad.md).

> Nota: la demo deja `worker3` registrado en `pg_dist_node`. Para volver al
> cluster base, quítalo con `citus_remove_node('worker3', 5432)` y detén el
> contenedor; o arranca siempre con `docker compose --profile scale up -d`.

## Solución de problemas

- **El puerto 5432 está ocupado por un PostgreSQL local.** Cambia
  `COORDINATOR_PORT` (y `DB_USUARIOS_PORT`) en `.env` y vuelve a levantar el
  stack. Si un Postgres local “ensombrece” `localhost:5432`, los scripts de host
  no conectarán al cluster; ejecútalos dentro de la imagen:
  ```bash
  docker compose run --rm --no-deps -v "$PWD/scripts:/scripts:ro" app \
    python /scripts/seed.py --reset --usuarios 1000 --pedidos 100000 --productos 50
  ```
  (En los contenedores, `CITUS_HOST` y `USUARIOS_HOST` ya apuntan a los servicios
  correctos.)
- **`docker compose up -d` tras haber escalado.** Si añadiste `worker3` y luego
  bajas el stack, arranca con `docker compose --profile scale up -d` o quita el
  nodo del cluster (ver `docs/escalabilidad.md`).
- **Datos sucios tras las pruebas.** `pytest` crea usuarios y pedidos de prueba;
  para dejar el dataset limpio vuelve a sembrar con `python scripts/seed.py --reset`.
- **`docker compose up -d --build app` se cuelga.** Si el contenedor `app` queda
  en estado `Created`, inícialo con `docker start shardshop-app-1` (el contenedor
  recreado conserva el mapeo de `8000`; evita `docker restart`, que puede perder
  el puerto).

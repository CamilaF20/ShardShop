#!/usr/bin/env bash
#
# init_cluster.sh — Inicializa el cluster Citus y el PostgreSQL de usuarios.
#
# Lo ejecuta el servicio de una sola vez `init-cluster` de docker-compose.
# Es IDEMPOTENTE: puede correrse varias veces sin efectos negativos.
#
# Concepto de Citus que ilustra: el registro de workers desde el coordinator
# mediante `citus_add_node`, que solo actualiza la metadata (pg_dist_node).
# No se usa `citus_set_coordinator_host` porque el coordinator no almacena
# shards (ver notas técnicas, sección 5 de PLAN.md).
#
set -euo pipefail

# --- Configuración desde variables de entorno (inyectadas por docker-compose) ---
PGUSER="${POSTGRES_USER:-postgres}"
export PGPASSWORD="${POSTGRES_PASSWORD:-postgres}"

COORDINATOR_HOST="${COORDINATOR_HOST:-coordinator}"
COORDINATOR_PORT="${COORDINATOR_PORT:-5432}"
COORDINATOR_DB="${COORDINATOR_DB:-postgres}"

USUARIOS_HOST="${USUARIOS_HOST:-db_usuarios}"
USUARIOS_PORT="${USUARIOS_PORT:-5432}"
USUARIOS_DB="${USUARIOS_DB:-postgres}"

WORKERS="${WORKERS:-worker1,worker2}"
WORKER_PORT="${WORKER_PORT:-5432}"

SQL_DIR="${SQL_DIR:-/sql}"

# --- Utilidades --------------------------------------------------------------
log() { printf '[init-cluster] %s\n' "$*"; }
err() { printf '[init-cluster][ERROR] %s\n' "$*" >&2; }

# Espera a que un servidor acepte conexiones, con reintentos.
wait_for_db() {
  local host="$1" port="$2" db="$3" intentos="${4:-30}"
  log "Esperando a ${host}:${port}/${db} ..."
  local i
  for ((i = 1; i <= intentos; i++)); do
    if pg_isready -h "$host" -p "$port" -U "$PGUSER" -d "$db" >/dev/null 2>&1; then
      log "${host}:${port} disponible (intento ${i})."
      return 0
    fi
    sleep 2
  done
  err "No se pudo conectar a ${host}:${port} tras ${intentos} intentos."
  return 1
}

psql_coordinator() {
  psql -h "$COORDINATOR_HOST" -p "$COORDINATOR_PORT" -U "$PGUSER" -d "$COORDINATOR_DB" \
       -v ON_ERROR_STOP=1 "$@"
}

# --- 1. Esperar a las bases ---------------------------------------------------
wait_for_db "$COORDINATOR_HOST" "$COORDINATOR_PORT" "$COORDINATOR_DB"
wait_for_db "$USUARIOS_HOST" "$USUARIOS_PORT" "$USUARIOS_DB"

# --- 2. Registrar los workers (idempotente) -----------------------------------
# La imagen oficial ya crea la extensión citus en la base `postgres`; la
# aseguramos por robustez.
psql_coordinator -c "CREATE EXTENSION IF NOT EXISTS citus;" >/dev/null

log "Registrando workers: ${WORKERS}"
IFS=',' read -ra WORKER_LIST <<< "$WORKERS"
for worker in "${WORKER_LIST[@]}"; do
  # Recorta espacios alrededor del nombre del worker.
  worker="$(echo "$worker" | xargs)"
  [ -z "$worker" ] && continue

  # ¿Ya está en la metadata? (pg_dist_node es la fuente autoritativa).
  ya_registrado="$(psql_coordinator -tAc \
    "SELECT count(*) FROM pg_dist_node WHERE nodename = '${worker}' AND nodeport = ${WORKER_PORT};")"

  if [ "$ya_registrado" -gt 0 ]; then
    log "Worker '${worker}' ya estaba registrado; se omite."
  else
    log "Añadiendo worker '${worker}' con citus_add_node ..."
    psql_coordinator -c "SELECT citus_add_node('${worker}', ${WORKER_PORT});"
  fi
done

# --- 3. Aplicar esquemas SQL (condicional por límite de fases) ----------------
# Si un archivo aún no existe (p. ej. al ejecutar solo la Fase 1), se avisa y
# se continúa; en cuanto exista, basta con re-ejecutar init-cluster.
aplicar_sql() {
  local archivo="$1" host="$2" puerto="$3" db="$4" nombre="$5"
  if [ -f "$archivo" ]; then
    log "Aplicando ${nombre} (${archivo}) ..."
    psql -h "$host" -p "$puerto" -U "$PGUSER" -d "$db" -v ON_ERROR_STOP=1 -f "$archivo"
  else
    log "AVISO: ${archivo} no existe todavía; se omite ${nombre}."
  fi
}

aplicar_sql "${SQL_DIR}/citus_schema.sql" \
  "$COORDINATOR_HOST" "$COORDINATOR_PORT" "$COORDINATOR_DB" \
  "esquema Citus (sharding horizontal)"

aplicar_sql "${SQL_DIR}/usuarios_schema.sql" \
  "$USUARIOS_HOST" "$USUARIOS_PORT" "$USUARIOS_DB" \
  "esquema de usuarios (sharding vertical)"

# --- 4. Resumen ---------------------------------------------------------------
# OJO: en Citus 12.1 la función devuelve las columnas `node_name` y `node_port`
# (no `nodename`/`nodeport`). Usarlas mal aborta el script por `set -e` aunque el
# cluster ya esté bien montado.
log "Workers activos:"
psql_coordinator -c "SELECT node_name, node_port FROM citus_get_active_worker_nodes() ORDER BY node_name;"
log "Inicialización completada."

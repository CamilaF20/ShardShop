#!/usr/bin/env bash
#
# demo_escalabilidad.sh — Escalabilidad horizontal y tolerancia a fallos (Fase 10).
#
# Recorre, con pausas (`read -p`), cuatro pasos:
#   1. Muestra la distribución actual de shards de `pedidos` por worker.
#   2. Levanta `worker3` (perfil "scale"), lo registra con `citus_add_node` y
#      reparte los shards con `rebalance_table_shards` (block_writes).
#   3. Muestra la nueva distribución (ahora entre 3 workers).
#   4. Detiene un worker (`docker stop`), muestra que las consultas que tocan sus
#      shards fallan mientras las demás siguen funcionando, y lo reinicia.
#
# Conceptos de Citus que ilustra: escalado horizontal añadiendo un nodo en
# caliente, rebalanceo de shards y el efecto de la caída de un worker cuando NO
# hay replicación de shards configurada.
#
# Uso (desde la raíz del repo):  ./scripts/demo_escalabilidad.sh
# Variables opcionales:
#   WORKER_NUEVO=worker3    WORKER_A_DETENER=worker2    N_CLIENTES=1000
#
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

PGUSER="${POSTGRES_USER:-postgres}"
PGDB="${POSTGRES_DB:-postgres}"
WORKER_NUEVO="${WORKER_NUEVO:-worker3}"
WORKER_A_DETENER="${WORKER_A_DETENER:-worker2}"
N_CLIENTES="${N_CLIENTES:-1000}"

# --- Utilidades --------------------------------------------------------------
log()  { printf '\n\033[1;34m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }
ok()   { printf '\033[1;32m[OK]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[FALLA]\033[0m %s\n' "$*"; }

pausa() { read -r -p "» $1 (Enter para continuar) " _ || true; }

# psql contra el coordinator (modo normal y modo "tuplas" -tAc).
psql_c()   { docker compose exec -T coordinator psql -U "$PGUSER" -d "$PGDB" "$@"; }
psql_c_t() { docker compose exec -T coordinator psql -U "$PGUSER" -d "$PGDB" -tAc "$1"; }

# Tabla de distribución: shards y filas de `pedidos` por worker.
# OJO: el `$$...$$` es dollar-quoting de Postgres; en bash va escapado (\$\$).
mostrar_distribucion() {
  psql_c -c "
SELECT cs.nodename,
       count(*)                              AS shards,
       coalesce(sum(r.result::bigint), 0)    AS filas
FROM citus_shards cs
LEFT JOIN run_command_on_shards('pedidos', \$\$SELECT count(*) FROM %s\$\$) r
       ON r.shardid = cs.shardid
WHERE cs.table_name = 'pedidos'::regclass
GROUP BY cs.nodename
ORDER BY cs.nodename;"
}

# Espera a que un worker acepte conexiones.
esperar_worker() {
  local worker="$1" i
  info "Esperando a que ${worker} esté listo ..."
  for i in $(seq 1 60); do
    if docker compose --profile scale exec -T "$worker" \
         pg_isready -U "$PGUSER" -d "$PGDB" >/dev/null 2>&1; then
      ok "${worker} disponible."
      return 0
    fi
    sleep 2
  done
  fail "${worker} no respondió a tiempo."
  return 1
}

# Busca un cliente cuyo shard viva (o no) en el worker indicado.
cliente_en_worker() {
  local worker="$1" excluir="$2"
  local filtro="="
  [ "$excluir" = "excluir" ] && filtro="<>"
  psql_c_t "
SELECT gs
FROM generate_series(1, ${N_CLIENTES}) gs
WHERE get_shard_id_for_distribution_column('pedidos', gs) IN (
        SELECT shardid FROM citus_shards
        WHERE nodename ${filtro} '${worker}'
          AND table_name = 'pedidos'::regclass)
LIMIT 1;"
}

# --- Paso 0: comprobar que el stack base está arriba -------------------------
log "Paso 0 — Comprobando el cluster"
if ! docker compose exec -T coordinator pg_isready -U "$PGUSER" -d "$PGDB" >/dev/null 2>&1; then
  fail "El coordinator no responde. Levanta el stack con: docker compose up -d"
  exit 1
fi
ok "Coordinator disponible."
psql_c -c "SELECT node_name, node_port FROM citus_get_active_worker_nodes() ORDER BY node_name;"
pausa "Distribución base (2 workers)"

# --- Paso 1: distribución actual ---------------------------------------------
log "Paso 1 — Distribución actual"
mostrar_distribucion
pausa "Añadir ${WORKER_NUEVO} y rebalancear"

# --- Paso 2: escalar con worker3 ---------------------------------------------
log "Paso 2 — Escalando el cluster con ${WORKER_NUEVO}"
info "Levantando ${WORKER_NUEVO} (perfil 'scale') ..."
docker compose --profile scale up -d "$WORKER_NUEVO"
esperar_worker "$WORKER_NUEVO"

ya="$(psql_c_t "SELECT count(*) FROM pg_dist_node WHERE nodename = '${WORKER_NUEVO}' AND nodeport = 5432;")"
if [ "${ya:-0}" -gt 0 ]; then
  info "${WORKER_NUEVO} ya estaba registrado en pg_dist_node."
else
  info "Registrando ${WORKER_NUEVO} con citus_add_node ..."
  psql_c -c "SELECT citus_add_node('${WORKER_NUEVO}', 5432);"
fi

# La reference table `productos` debe existir en el nuevo worker para que los
# JOIN en sus shards se resuelvan localmente.
info "Replicando reference tables en el nuevo nodo ..."
psql_c -c "SELECT replicate_reference_tables();" >/dev/null 2>&1 || \
  info "(Citus replica las reference tables automáticamente al añadir el nodo)"

info "Rebalanceando shards de 'pedidos' (block_writes) ..."
psql_c -c "SELECT rebalance_table_shards('pedidos', shard_transfer_mode := 'block_writes');"
ok "Rebalanceo completado."
pausa "Ver nueva distribución"

# --- Paso 3: nueva distribución (3 workers) ----------------------------------
log "Paso 3 — Nueva distribución"
psql_c -c "SELECT node_name, node_port FROM citus_get_active_worker_nodes() ORDER BY node_name;"
mostrar_distribucion
pausa "Probar tolerancia a fallos deteniendo ${WORKER_A_DETENER}"

# --- Paso 4: tolerancia a fallos ---------------------------------------------
log "Paso 4 — Tolerancia a fallos: deteniendo ${WORKER_A_DETENER}"

CLIENTE_AFECTADO="$(cliente_en_worker "$WORKER_A_DETENER" "")"
CLIENTE_SEGURO="$(cliente_en_worker "$WORKER_A_DETENER" excluir)"

if [ -z "$CLIENTE_AFECTADO" ] || [ -z "$CLIENTE_SEGURO" ]; then
  fail "No se pudieron localizar clientes de prueba (¿hay datos? ejecuta scripts/seed.py)."
  exit 1
fi
info "Cliente con shard en ${WORKER_A_DETENER}: #${CLIENTE_AFECTADO}"
info "Cliente con shard en otro worker:     #${CLIENTE_SEGURO}"

info "Deteniendo ${WORKER_A_DETENER} ..."
docker compose --profile scale stop "$WORKER_A_DETENER" >/dev/null
sleep 2

# 4a) Consulta que toca el worker caído -> debe fallar.
salida_falla="$(psql_c -c "SELECT count(*) FROM pedidos WHERE cliente_id = ${CLIENTE_AFECTADO};" 2>&1)"
rc_falla=$?
if [ "$rc_falla" -ne 0 ]; then
  ok "La consulta al shard de ${WORKER_A_DETENER} FALLA como se espera:"
  printf '%s\n' "$salida_falla" | grep -iE "error|failed|no route|could not|connection" | head -n 2 | sed 's/^/     /'
else
  fail "Se esperaba un fallo en la consulta al worker caído, pero respondió:"
  printf '     %s\n' "$salida_falla"
fi

# 4b) Consulta a un shard de otro worker -> debe seguir funcionando.
valor_ok="$(psql_c_t "SELECT count(*) FROM pedidos WHERE cliente_id = ${CLIENTE_SEGURO};")"
if [ $? -eq 0 ] && [ -n "$valor_ok" ]; then
  ok "Una consulta a un shard de otro worker SIGUE funcionando:"
  printf '     cliente #%s -> %s pedidos\n' "$CLIENTE_SEGURO" "$valor_ok"
else
  fail "Una consulta a un worker sano también falló (inesperado)."
fi

# 4c) Reiniciar el worker caído.
info "Reiniciando ${WORKER_A_DETENER} ..."
docker compose --profile scale start "$WORKER_A_DETENER" >/dev/null
esperar_worker "$WORKER_A_DETENER"
valor_rec="$(psql_c_t "SELECT count(*) FROM pedidos WHERE cliente_id = ${CLIENTE_AFECTADO};")"
if [ $? -eq 0 ] && [ -n "$valor_rec" ]; then
  ok "Tras el reinicio, el shard recuperado vuelve a responder (${valor_rec} pedidos)."
else
  fail "La consulta sigue fallando tras reiniciar."
fi

# --- Cierre ------------------------------------------------------------------
log "Resumen"
mostrar_distribucion
cat <<'FIN'

Notas:
  - Sin replicación de shards, un worker caído inutiliza SOLO sus shards.
  - El rebalanceo usó shard_transfer_mode := 'block_writes' (no exige wal_level=logical).
  - Para volver al cluster base de 2 workers:
      docker compose exec -T coordinator psql -U postgres -d postgres \
        -c "SELECT citus_remove_node('worker3', 5432);"
      docker compose --profile scale stop worker3
    (o mantén 3 workers arrancando siempre con: docker compose --profile scale up -d)
FIN
ok "Demostración completada."

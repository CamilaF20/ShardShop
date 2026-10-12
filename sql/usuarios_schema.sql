-- =============================================================================
-- usuarios_schema.sql — ShardShop
-- =============================================================================
-- Esquema de `db_usuarios`: PostgreSQL 16 INDEPENDIENTE (sin Citus).
--
-- Este archivo ilustra el SHARDING VERTICAL mediante DOS técnicas, ambas
-- manuales (ningún motor las hace de forma automática):
--
--   1. VERTICAL POR DOMINIO (servidor distinto): los usuarios viven en un
--      PostgreSQL aparte (`db_usuarios`), desacoplados del cluster Citus que
--      guarda pedidos/productos. El dominio "identidad" escala y se respalda
--      de forma independiente al dominio transaccional.
--
--   2. VERTICAL POR COLUMNAS (tabla aparte): dentro de ese dominio se separan
--      las columnas CALIENTES de las PESADAS en dos tablas 1:1.
--        - `usuarios`        -> columnas calientes y ligeras (id, nombre, email,
--                               creado_en): las que se leen casi siempre.
--        - `usuarios_perfil` -> columnas pesadas y de acceso esporádico
--                               (biografia text, foto bytea, preferencias jsonb):
--                               filas grandes que inflan la tabla y por eso se
--                               aíslan, para que las lecturas "calientes" no
--                               paguen su costo de I/O.
--
-- Consejo conjunto: como pedidos.cliente_id y usuarios.id están en bases
-- distintas, NO puede existir una FK entre ambos. La app valida la existencia
-- del usuario antes de crear el pedido. Es una desventaja real del sharding
-- vertical (pierde la integridad referencial entre dominios).
--
-- Idempotente: usa CREATE TABLE IF NOT EXISTS.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. usuarios — columnas calientes (sharding vertical por dominio + columnas)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS usuarios (
    id        serial PRIMARY KEY,
    nombre    text NOT NULL,
    email     text NOT NULL UNIQUE,
    creado_en timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- 2. usuarios_perfil — columnas pesadas aisladas (sharding vertical por columnas)
-- -----------------------------------------------------------------------------
-- La FK 1:1 con ON DELETE CASCADE se mantiene porque AMBAS tablas viven en la
-- MISMA base (`db_usuarios`): aquí sí podemos conservar la integridad
-- referencial, a diferencia de la relación entre bases distintas.
CREATE TABLE IF NOT EXISTS usuarios_perfil (
    usuario_id   int PRIMARY KEY REFERENCES usuarios (id) ON DELETE CASCADE,
    biografia    text,
    foto         bytea,
    preferencias jsonb
);

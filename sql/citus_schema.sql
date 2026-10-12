-- =============================================================================
-- citus_schema.sql — ShardShop
-- =============================================================================
-- Esquema del cluster Citus: ilustra el SHARDING HORIZONTAL.
--
--   productos        -> REFERENCE TABLE  (copia completa replicada en cada worker)
--   pedidos          -> tabla DISTRIBUIDA por cliente_id
--   detalle_pedidos  -> tabla DISTRIBUIDA por cliente_id y CO-LOCALIZADA con pedidos
--
-- Se aplica desde el coordinator (docker-compose, servicio `init-cluster`).
-- Es IDEMPOTENTE: puede re-ejecutarse sin efectos negativos (guardas sobre
-- pg_dist_partition antes de distribuir).
--
-- Orden de distribución IMPORTANTE:
--   1. `productos` como reference table, para que detalle_pedidos pueda tener
--      una FK hacia ella (una distribuida puede referenciar una reference).
--   2. `pedidos` distribuida por cliente_id.
--   3. `detalle_pedidos` distribuida por cliente_id con colocate_with => 'pedidos',
--      para que sus JOIN por cliente_id ocurran dentro de un único shard.
--   4. Las FK se AÑADEN AL FINAL: Citus exige que la tabla referenciada ya esté
--      distribuida/co-localizada en el momento de crear la restricción.
-- =============================================================================

-- La imagen oficial ya crea la extensión en la base `postgres`; se asegura por
-- robustez en el coordinator (los workers la traen de fábrica).
CREATE EXTENSION IF NOT EXISTS citus;

-- -----------------------------------------------------------------------------
-- 1. productos — REFERENCE TABLE
-- -----------------------------------------------------------------------------
-- Decisión: catálogo pequeño y leído por TODOS los pedidos de cualquier cliente.
-- Al ser una reference table, Citus replica una copia completa en cada worker,
-- de modo que los JOIN `detalle_pedidos JOIN productos` se resuelven localmente
-- en el shard (sin viaje de red al coordinator ni shards distribuidos).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS productos (
    id     serial PRIMARY KEY,
    nombre text NOT NULL,
    precio numeric(10,2) NOT NULL CHECK (precio >= 0),
    stock  int NOT NULL DEFAULT 0 CHECK (stock >= 0)
);

-- -----------------------------------------------------------------------------
-- 2. pedidos — tabla DISTRIBUIDA por cliente_id (sharding horizontal)
-- -----------------------------------------------------------------------------
-- Decisión de la clave de distribución: `cliente_id`.
--   - Alta cardinalidad (muchos clientes) => buena distribución entre shards.
--   - La mayoría de las consultas filtran por cliente => poda de shards.
--   - `id` es bigserial, pero NO es único a nivel global: cada shard mantiene su
--     propia secuencia. Por eso la PK DEBE incluir la clave de distribución y es
--     compuesta: PRIMARY KEY (cliente_id, id). Citus exige que la clave de
--     distribución forme parte de la PK y de todo UNIQUE.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pedidos (
    id         bigserial,
    cliente_id int NOT NULL,
    fecha      timestamptz NOT NULL DEFAULT now(),
    estado     text NOT NULL DEFAULT 'pendiente',
    total      numeric(10,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (cliente_id, id)
);

-- -----------------------------------------------------------------------------
-- 3. detalle_pedidos — tabla DISTRIBUIDA y CO-LOCALIZADA con pedidos
-- -----------------------------------------------------------------------------
-- Decisión: misma clave de distribución `cliente_id` que pedidos.
--   - Co-localización => las filas de un pedido y su detalle viven en el MISMO
--     shard. Así el JOIN por cliente_id es local (un solo task) y una compra
--     completa se inserta en una transacción local a un shard (sin 2PC).
--   - Igual que en pedidos, la PK incluye cliente_id: PRIMARY KEY (cliente_id, id).
--   - `pedido_id` referencia la PK compuesta de pedidos, por lo que la FK debe
--     arrastrar también cliente_id: FOREIGN KEY (cliente_id, pedido_id).
--     La FK a productos(id) es distribuida -> reference, permitida por Citus.
--     (Las FK se añaden al final, tras distribuir; ver más abajo.)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS detalle_pedidos (
    id              bigserial,
    cliente_id      int NOT NULL,
    pedido_id       bigint NOT NULL,
    producto_id     int NOT NULL,
    cantidad        int NOT NULL CHECK (cantidad > 0),
    precio_unitario numeric(10,2) NOT NULL,
    PRIMARY KEY (cliente_id, id)
);

-- -----------------------------------------------------------------------------
-- 4. Distribución de tablas (con guardas de idempotencia)
-- -----------------------------------------------------------------------------
-- Se consulta pg_dist_partition (metadata de Citus) para no volver a distribuir
-- una tabla ya distribuida: `create_*_table` falla si la tabla ya lo está.

-- 4.a productos -> reference table (replicada en cada worker).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'productos'::regclass
    ) THEN
        PERFORM create_reference_table('productos');
    END IF;
END
$$;

-- 4.b pedidos -> distribuida por cliente_id.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'pedidos'::regclass
    ) THEN
        PERFORM create_distributed_table('pedidos', 'cliente_id');
    END IF;
END
$$;

-- 4.c detalle_pedidos -> distribuida por cliente_id, CO-LOCALIZADA con pedidos.
-- `colocate_with => 'pedidos'` garantiza el mismo colocation group (mismo
-- colocationid), requisito para que las FK y los JOIN locales funcionen.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_dist_partition WHERE logicalrelid = 'detalle_pedidos'::regclass
    ) THEN
        PERFORM create_distributed_table(
            'detalle_pedidos', 'cliente_id', colocate_with => 'pedidos'
        );
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- 5. Restricciones FOREIGN KEY (tras distribuir y co-localizar)
-- -----------------------------------------------------------------------------
-- Decisión: se añaden AQUÍ y no en el CREATE TABLE porque Citus valida que la
-- tabla referenciada esté distribuida/co-localizada al crear la FK. Ahora ya lo
-- están: pedidos (colocation de detalle_pedidos) y productos (reference).

-- FK pedido: (cliente_id, pedido_id) -> pedidos(cliente_id, id).
-- Incluye la clave de distribución porque la PK referenciada es compuesta.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'detalle_pedidos_pedido_fk'
    ) THEN
        ALTER TABLE detalle_pedidos
            ADD CONSTRAINT detalle_pedidos_pedido_fk
            FOREIGN KEY (cliente_id, pedido_id) REFERENCES pedidos (cliente_id, id);
    END IF;
END
$$;

-- FK producto -> productos(id): permitida por ser productos una reference table.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'detalle_pedidos_producto_fk'
    ) THEN
        ALTER TABLE detalle_pedidos
            ADD CONSTRAINT detalle_pedidos_producto_fk
            FOREIGN KEY (producto_id) REFERENCES productos (id);
    END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- 6. Índice para el patrón de acceso más frecuente
-- -----------------------------------------------------------------------------
-- "pedidos de un cliente ordenados por fecha". Al incluir la clave de
-- distribución como primera columna, el índice se crea por shard y la consulta
-- filtrada por cliente poda a un único shard.
CREATE INDEX IF NOT EXISTS idx_pedidos_cliente_fecha
    ON pedidos (cliente_id, fecha);

"""Configuración de ShardShop cargada desde variables de entorno.

Ilustra el sharding VERTICAL a nivel de configuración: la app mantiene dos
destinos de conexión independientes, uno por dominio.

- `CITUS_*`     -> cluster Citus (sharding horizontal: productos/pedidos).
- `USUARIOS_*`  -> PostgreSQL `db_usuarios` (sharding vertical: usuarios).

Los `defaults` apuntan a `localhost` con los puertos publicados por
docker-compose, de modo que se puede ejecutar `uvicorn` fuera de Docker.
Dentro de Docker, compose inyecta los nombres de servicio (`coordinator`,
`db_usuarios`). Credenciales y puertos NUNCA se hardcodean: vienen del entorno.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Ajustes de la aplicación, resueltos desde el entorno o `.env`."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Cluster Citus (sharding horizontal) ---
    citus_host: str = "localhost"
    citus_port: int = 5432
    citus_user: str = "postgres"
    citus_password: str = "postgres"
    citus_db: str = "postgres"

    # --- PostgreSQL de usuarios (sharding vertical) ---
    usuarios_host: str = "localhost"
    usuarios_port: int = 5433
    usuarios_user: str = "postgres"
    usuarios_password: str = "postgres"
    usuarios_db: str = "postgres"

    def citus_conninfo(self) -> str:
        """Cadena de conexión (libpq) para el coordinator de Citus."""
        return (
            f"host={self.citus_host} port={self.citus_port} "
            f"user={self.citus_user} password={self.citus_password} "
            f"dbname={self.citus_db}"
        )

    def usuarios_conninfo(self) -> str:
        """Cadena de conexión (libpq) para el PostgreSQL de usuarios."""
        return (
            f"host={self.usuarios_host} port={self.usuarios_port} "
            f"user={self.usuarios_user} password={self.usuarios_password} "
            f"dbname={self.usuarios_db}"
        )


# Instancia única reutilizada por `db.py` y los routers.
settings = Settings()

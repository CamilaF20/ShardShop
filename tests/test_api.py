"""tests/test_api.py — Pruebas de integración de la API (Fase 9).

Usa `pytest` + `TestClient` de FastAPI. Al entrar al contexto del cliente se
ejecuta el `lifespan` de la app, que abre los dos pools (Citus y db_usuarios),
por lo que se requiere el stack levantado y sembrado (`scripts/seed.py`).

Cubre: `/health`, creación y lectura de usuario, creación de pedido, 404 con
cliente inexistente y `/estadisticas`.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture(scope="module")
def cliente() -> TestClient:
    """Cliente único para el módulo (abre/cierra los pools una sola vez)."""
    with TestClient(app) as c:
        yield c


def test_health(cliente: TestClient) -> None:
    """`/health` responde ok y reporta los dos workers del cluster."""
    r = cliente.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["citus"]["ok"] is True
    assert data["usuarios"]["ok"] is True
    assert len(data["workers"]) == 2


def test_crear_y_leer_usuario(cliente: TestClient) -> None:
    """Crea un usuario (columnas caliente+pesada) y lo relee; el duplicado da 409."""
    email = f"test-{uuid.uuid4().hex}@mail.com"
    r = cliente.post(
        "/usuarios",
        json={
            "nombre": "Test",
            "email": email,
            "biografia": "usuario de prueba",
            "preferencias": {"tema": "claro"},
        },
    )
    assert r.status_code == 201, r.text
    usuario_id = r.json()["id"]

    r2 = cliente.get(f"/usuarios/{usuario_id}")
    assert r2.status_code == 200
    assert r2.json()["email"] == email

    r3 = cliente.post("/usuarios", json={"nombre": "Test", "email": email})
    assert r3.status_code == 409


def test_crear_pedido(cliente: TestClient) -> None:
    """Crea un pedido para un cliente existente y lo recupera co-localizado."""
    email = f"compra-{uuid.uuid4().hex}@mail.com"
    cliente_id = cliente.post("/usuarios", json={"nombre": "Comprador", "email": email}).json()["id"]
    producto_id = cliente.get("/productos").json()[0]["id"]

    r = cliente.post(
        "/pedidos",
        json={"cliente_id": cliente_id, "items": [{"producto_id": producto_id, "cantidad": 2}]},
    )
    assert r.status_code == 201, r.text
    pedido = r.json()
    assert pedido["cliente_id"] == cliente_id
    assert pedido["total"] > 0

    r2 = cliente.get(f"/clientes/{cliente_id}/pedidos")
    assert r2.status_code == 200
    assert any(p["id"] == pedido["id"] for p in r2.json())


def test_pedido_cliente_inexistente(cliente: TestClient) -> None:
    """Un pedido para un cliente inexistente devuelve 404 (no hay FK entre bases)."""
    r = cliente.post(
        "/pedidos",
        json={"cliente_id": 999999999, "items": [{"producto_id": 1, "cantidad": 1}]},
    )
    assert r.status_code == 404


def test_estadisticas(cliente: TestClient) -> None:
    """`/estadisticas` agrega en todos los shards y devuelve el top de productos."""
    r = cliente.get("/estadisticas")
    assert r.status_code == 200
    data = r.json()
    assert data["total_pedidos"] >= 1
    assert isinstance(data["top_productos"], list)

from __future__ import annotations

import pytest

from src.env_config import load_neo4j_env_config


def test_load_neo4j_env_config_returns_none_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NEO4J_URI", raising=False)
    monkeypatch.delenv("NEO4J_USERNAME", raising=False)
    monkeypatch.delenv("NEO4J_PASSWORD", raising=False)

    assert load_neo4j_env_config() is None


def test_load_neo4j_env_config_raises_on_partial_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEO4J_URI", "neo4j://localhost:7687")
    monkeypatch.delenv("NEO4J_USERNAME", raising=False)
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")

    with pytest.raises(ValueError):
        load_neo4j_env_config()


def test_load_neo4j_env_config_loads_complete_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NEO4J_URI", "neo4j://localhost:7687")
    monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")

    config = load_neo4j_env_config()
    assert config is not None
    assert config.uri == "neo4j://localhost:7687"
    assert config.username == "neo4j"
    assert config.password == "secret"

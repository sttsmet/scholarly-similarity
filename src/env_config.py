from __future__ import annotations

import os
from pydantic import BaseModel, ConfigDict


class Neo4jEnvConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    uri: str
    username: str
    password: str


def load_neo4j_env_config() -> Neo4jEnvConfig | None:
    """Load Neo4j credentials from environment variables.

    Returns None when no Neo4j variables are set. Raises ValueError when a
    partial configuration is provided.
    """

    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    if not any((uri, username, password)):
        return None

    missing = []
    if not uri:
        missing.append("NEO4J_URI")
    if not username:
        missing.append("NEO4J_USERNAME")
    if not password:
        missing.append("NEO4J_PASSWORD")

    if missing:
        raise ValueError(f"Incomplete Neo4j environment configuration; missing: {', '.join(missing)}")

    return Neo4jEnvConfig(uri=uri, username=username, password=password)

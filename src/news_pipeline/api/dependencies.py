from __future__ import annotations

from collections.abc import Generator

import psycopg
from pydantic_settings import BaseSettings


class ApiSettings(BaseSettings):
    database_url: str = "postgresql://news:news@localhost:5432/news"


settings = ApiSettings()


def get_db_connection() -> Generator[psycopg.Connection, None, None]:
    with psycopg.connect(settings.database_url) as conn:
        yield conn


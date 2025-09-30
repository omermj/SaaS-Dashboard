import os, psycopg2
from functools import lru_cache
from sqlalchemy import create_engine


@lru_cache(maxsize=1)
def _dsn():
    """Get the database connection string from environment variable."""
    dsn = os.getenv("DATABASE_URL")
    if dsn is None:
        raise ValueError("DATABASE_URL environment variable not set")
    return dsn


def get_engine():
    """Get a new database connection using the cached DSN."""
    # return psycopg2.connect(_dsn())
    return create_engine(_dsn(), pool_pre_ping=True, future=True)


def get_conn():
    """Get a new database connection using the cached DSN."""
    return get_engine().connect()

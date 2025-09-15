import os, psycopg2
from functools import lru_cache


@lru_cache
def _dsn(maxsize=None):
    """Get the database connection string from environment variable."""
    dsn = os.getenv("DATABASE_URL")
    if dsn is None:
        raise ValueError("DATABASE_URL environment variable not set")
    return dsn


def get_conn():
    """Get a new database connection using the cached DSN."""
    return psycopg2.connect(_dsn())

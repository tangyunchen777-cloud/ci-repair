import ssl

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.config import Settings


def make_engine(settings: Settings):
    settings.require("database_url")
    url = make_url(settings.database_url.get_secret_value())
    if url.drivername != "postgresql+asyncpg":
        raise ValueError("DATABASE_URL must use postgresql+asyncpg")
    # asyncpg accepts an SSLContext, not libpq's sslmode query parameter.
    if "sslmode" in url.query:
        raise ValueError("Use DATABASE_SSL and DATABASE_SSL_CA instead of sslmode")
    tls = ssl.create_default_context(cafile=settings.database_ssl_ca) if settings.database_ssl else False
    return create_async_engine(
        url, pool_pre_ping=True, hide_parameters=True,
        connect_args={"ssl": tls, "timeout": 10, "command_timeout": 15},
    )


def make_sessions(engine):
    return async_sessionmaker(engine, expire_on_commit=False)

import asyncio

from alembic import context

from app.config import get_settings
from app.db import make_engine
from app.db_models import Base


def run_migrations(connection):
    context.configure(connection=connection, target_metadata=Base.metadata, compare_type=True)
    with context.begin_transaction():
        context.run_migrations()


async def online():
    engine = make_engine(get_settings())
    try:
        async with engine.connect() as connection:
            await connection.run_sync(run_migrations)
    finally:
        await engine.dispose()


if context.is_offline_mode():
    raise RuntimeError("Run migrations online inside the migration container")
else:
    asyncio.run(online())

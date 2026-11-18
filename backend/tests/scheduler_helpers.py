"""Two P2S printers in an in-memory database, and jobs to queue on them.

Shared by scheduler tests that run real ``check_queue`` passes against it.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import backend.app.models  # noqa: F401 - populate Base.metadata
from backend.app.core.database import Base
from backend.app.models.library import LibraryFile
from backend.app.models.print_queue import PrintQueueItem
from backend.app.models.printer import Printer


@asynccontextmanager
async def two_p2s_printers():
    """Yields a namespace with ``session_maker``; printers 1 and 2 are P2S-01/02."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    async with session_maker() as db:
        for pid in (1, 2):
            db.add(
                Printer(
                    id=pid,
                    name=f"P2S-0{pid}",
                    serial_number=f"P2S000{pid}",
                    ip_address=f"10.0.0.{pid}",
                    access_code="x",
                    model="P2S",
                    is_active=True,
                )
            )
        await db.commit()

    try:
        yield SimpleNamespace(session_maker=session_maker)
    finally:
        await engine.dispose()


async def add_job(ctx, *, printer_id=None, target_model=None, manual_start=False) -> int:
    """Queue a pending P2S library job; returns its id."""
    async with ctx.session_maker() as db:
        lib = LibraryFile(
            filename="job.gcode.3mf",
            file_path="/library/job.gcode.3mf",
            file_size=10,
            file_type="gcode.3mf",
            file_metadata={"sliced_for_model": "P2S"},
        )
        db.add(lib)
        await db.flush()
        item = PrintQueueItem(
            status="pending",
            position=1,
            printer_id=printer_id,
            target_model=target_model,
            library_file_id=lib.id,
            manual_start=manual_start,
        )
        db.add(item)
        await db.commit()
        return item.id

"""One fixed worker window per server process, independent of HTTP requests."""

import asyncio
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

logger = logging.getLogger(__name__)


class WorkerSupervisor:
    def __init__(
        self,
        *,
        lifetime: float = 300,
        shutdown_grace: float = 5,
        command: Sequence[str] | None = None,
    ) -> None:
        # Shorter windows and alternate children make lifecycle tests fast.
        if not 0 < shutdown_grace < lifetime <= 300:
            raise ValueError("require 0 < shutdown_grace < lifetime <= 300 seconds")
        self._lifetime = lifetime
        self._shutdown_grace = shutdown_grace
        self._command = tuple(command) if command is not None else (
            sys.executable,
            "-u",
            str(Path(__file__).with_name("worker.py")),
        )
        self._shutdown = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> bool:
        """Reserve a window without awaiting process creation or connectivity.

        Called only on the server's event loop. No await between checking and
        assigning the task means simultaneous requests cannot create duplicates.
        """
        if self._shutdown.is_set():
            return False
        if self._task is None or self._task.done():
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._lifetime
            self._task = loop.create_task(self._supervise(deadline))
        return True

    def request_shutdown(self) -> None:
        self._shutdown.set()

    async def close(self) -> None:
        self.request_shutdown()
        if self._task is not None:
            # Do not cancel process creation: a spawned child must be reaped.
            await asyncio.shield(self._task)

    async def _supervise(self, deadline: float) -> None:
        process = None
        wait_task = None
        stop_task = None
        loop = asyncio.get_running_loop()
        try:
            if self._shutdown.is_set():
                return
            process = await asyncio.create_subprocess_exec(*self._command)
            logger.info("Background worker spawned pid=%s", process.pid)
            wait_task = asyncio.create_task(process.wait())
            stop_task = asyncio.create_task(self._shutdown.wait())
            done, _ = await asyncio.wait(
                (wait_task, stop_task),
                timeout=max(0, deadline - self._shutdown_grace - loop.time()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if wait_task in done:
                logger.log(
                    logging.INFO if process.returncode == 0 else logging.ERROR,
                    "Background worker exited pid=%s returncode=%s",
                    process.pid,
                    process.returncode,
                )
                return

            logger.info("Stopping background worker pid=%s", process.pid)
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            grace = min(self._shutdown_grace, max(0, deadline - loop.time()))
            try:
                await asyncio.wait_for(asyncio.shield(wait_task), timeout=grace)
            except TimeoutError:
                logger.warning("Worker shutdown deadline reached pid=%s", process.pid)
        except Exception:
            logger.exception("Background worker failed")
        finally:
            # Also clean up on unexpected supervisor errors or cancellation.
            if process is not None:
                if process.returncode is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                await process.wait()
            for task in (wait_task, stop_task):
                if task is not None:
                    task.cancel()
            await asyncio.gather(
                *(task for task in (wait_task, stop_task) if task is not None),
                return_exceptions=True,
            )

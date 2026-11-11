import asyncio
import signal
import sys
from functools import partial
from unittest.mock import AsyncMock

import httpx
import pytest

import main
from main import create_app
from supervisor import WorkerSupervisor


@pytest.fixture(autouse=True)
def disable_telemetry(monkeypatch):
    # Unit tests must not export telemetry using the developer's environment.
    monkeypatch.setattr(
        main, "InvocationAgentServerHost",
        partial(main.InvocationAgentServerHost, configure_observability=None),
    )


@pytest.fixture
def children(monkeypatch):
    processes = []
    spawn = asyncio.create_subprocess_exec

    async def tracked_spawn(*args, **kwargs):
        process = await spawn(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", tracked_spawn)
    return processes


def supervisor_for(code, **kwargs):
    return WorkerSupervisor(
        command=[sys.executable, "-u", "-c", code],
        lifetime=1,
        shutdown_grace=0.2,
        **kwargs,
    )


async def wait_until(predicate):
    async with asyncio.timeout(5):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_handler_acknowledges_while_spawn_is_blocked(monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    real_spawn = asyncio.create_subprocess_exec

    async def blocked_spawn(*args):
        entered.set()
        await release.wait()
        return await real_spawn(*args)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", blocked_spawn)
    supervisor = supervisor_for("import time; time.sleep(60)")
    app = create_app(supervisor)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            async with asyncio.timeout(1):
                responses = await asyncio.gather(
                    *(client.post("/invocations") for _ in range(20))
                )
            assert all(response.status_code == 202 for response in responses)
            assert all(response.json() == {"status": "accepted"} for response in responses)
            await entered.wait()
            assert not release.is_set()
            assert (await client.get("/readiness")).status_code == 200
            supervisor.request_shutdown()
            assert (await client.post("/invocations")).status_code == 503
    finally:
        release.set()
        await supervisor.close()


async def test_single_child_fixed_window_and_restart(children):
    supervisor = supervisor_for("import time; time.sleep(60)")
    try:
        started = asyncio.get_running_loop().time()
        assert supervisor.start()
        await wait_until(lambda: len(children) == 1)
        original = supervisor._task
        await asyncio.sleep(0.5)
        for _ in range(20):
            assert supervisor.start()
        assert supervisor._task is original
        await asyncio.wait_for(asyncio.shield(original), 1)
        assert asyncio.get_running_loop().time() - started < 1.5
        assert len(children) == 1
        assert children[0].returncode == -signal.SIGTERM
        assert supervisor.start()
        await wait_until(lambda: len(children) == 2)
    finally:
        await supervisor.close()
    assert children[1].returncode is not None


async def test_unresponsive_child_is_killed_and_reaped(children, tmp_path, caplog):
    ready = tmp_path / "ready"
    supervisor = supervisor_for(
        "import signal,time; from pathlib import Path; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        f"Path({str(ready)!r}).touch(); time.sleep(60)"
    )
    try:
        supervisor.start()
        await wait_until(ready.exists)
        await asyncio.wait_for(asyncio.shield(supervisor._task), 2)
        assert children[0].returncode == -signal.SIGKILL
        assert "shutdown deadline reached" in caplog.text
    finally:
        await supervisor.close()


async def test_early_failure_does_not_auto_restart(children, caplog):
    supervisor = supervisor_for("raise SystemExit(7)")
    try:
        supervisor.start()
        await asyncio.wait_for(asyncio.shield(supervisor._task), 2)
        assert children[0].returncode == 7
        assert "returncode=7" in caplog.text
        await asyncio.sleep(0.05)
        assert len(children) == 1
        supervisor.start()
        await asyncio.wait_for(asyncio.shield(supervisor._task), 2)
        assert len(children) == 2
    finally:
        await supervisor.close()


async def test_spawn_failure_is_logged(monkeypatch, caplog):
    spawn = AsyncMock(side_effect=OSError("cannot spawn"))
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    supervisor = WorkerSupervisor()
    supervisor.start()
    await supervisor._task
    assert "Background worker failed" in caplog.text
    assert spawn.await_count == 1
    await supervisor.close()


async def test_shutdown_hook_terminates_child(children):
    supervisor = supervisor_for("import time; time.sleep(60)")
    app = create_app(supervisor)
    async with app.router.lifespan_context(app):
        supervisor.start()
        await wait_until(lambda: len(children) == 1)
    assert children[0].returncode is not None
    assert not supervisor.start()


@pytest.mark.parametrize("lifetime,grace", [(301, 5), (0, 0), (1, 1), (1, -1)])
def test_invalid_lifetime(lifetime, grace):
    with pytest.raises(ValueError):
        WorkerSupervisor(lifetime=lifetime, shutdown_grace=grace)

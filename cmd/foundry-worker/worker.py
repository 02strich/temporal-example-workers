"""Child process entry point; lifetime is enforced by the parent supervisor."""

import asyncio
import logging
import os
import signal
from datetime import timedelta

from temporalio.client import Client
from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from activities import noop_activity
from workflows import NoopWorkflow

logger = logging.getLogger(__name__)


def env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def create_worker(client: Client) -> Worker:
    return Worker(
        client,
        task_queue=env("TQ_NAME", "worker-versioning-sample"),
        workflows=[NoopWorkflow],
        activities=[noop_activity],
        disable_eager_activity_execution=True,
        max_concurrent_activities=2,
        max_concurrent_workflow_tasks=2,
        max_concurrent_nexus_tasks=2,
        graceful_shutdown_timeout=timedelta(seconds=4),
        deployment_config=WorkerDeploymentConfig(
            version=WorkerDeploymentVersion(
                deployment_name=env("DEPLOYMENT_NAME", "test"),
                build_id=env("BUILD_ID", "v1"),
            ),
            use_worker_versioning=True,
            default_versioning_behavior=VersioningBehavior.PINNED,
        ),
    )


async def run_worker() -> None:
    host = env("HOST_PORT", "127.0.0.1:7233")
    namespace = env("NAMESPACE", "default")
    logger.info(
        "Connecting worker host=%s namespace=%s task_queue=%s",
        host, namespace, env("TQ_NAME", "worker-versioning-sample"),
    )
    client = await Client.connect(
        host,
        namespace=namespace,
        tls=bool(os.environ.get("ENABLE_TLS")),
        api_key=os.environ.get("API_KEY") or None,
    )
    async with create_worker(client):
        logger.info("Temporal worker started")
        await asyncio.Event().wait()


async def main() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    execution = asyncio.create_task(run_worker())
    stopping = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            (execution, stopping), return_when=asyncio.FIRST_COMPLETED
        )
        if execution in done:
            await execution  # Propagate connection failures and fatal SDK errors.
        else:
            logger.info("Worker shutdown requested")
    finally:
        execution.cancel()
        stopping.cancel()
        results = await asyncio.gather(execution, stopping, return_exceptions=True)
        # Shutdown errors should produce a nonzero exit, not disappear in gather.
        if isinstance(results[0], Exception):
            raise results[0]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())

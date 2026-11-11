import asyncio
import os
from datetime import timedelta
from uuid import uuid4

import pytest
from temporalio.api.workflowservice.v1 import SetWorkerDeploymentCurrentVersionRequest
from temporalio.client import Client
from temporalio.service import RPCError

from worker import create_worker


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("TEMPORAL_TEST_ADDRESS"),
    reason="set TEMPORAL_TEST_ADDRESS to a local Temporal dev server",
)
async def test_dynamic_workflows_and_activities(monkeypatch):
    suffix = uuid4().hex
    deployment = f"foundry-test-{suffix}"
    queue = f"foundry-test-{suffix}"
    monkeypatch.setenv("DEPLOYMENT_NAME", deployment)
    monkeypatch.setenv("BUILD_ID", "v1")
    monkeypatch.setenv("TQ_NAME", queue)
    client = await Client.connect(os.environ["TEMPORAL_TEST_ADDRESS"])
    async with create_worker(client):
        # Pollers register their version asynchronously. Route new workflows to it.
        async with asyncio.timeout(20):
            while True:
                try:
                    await client.workflow_service.set_worker_deployment_current_version(
                        SetWorkerDeploymentCurrentVersionRequest(
                            namespace="default", deployment_name=deployment,
                            build_id="v1", identity="foundry-integration-test",
                        )
                    )
                    break
                except RPCError:
                    await asyncio.sleep(0.2)
        handles = [
            await client.start_workflow(
                name, args=[{"ignored": True}, 123], id=f"{name}-{suffix}",
                task_queue=queue, execution_timeout=timedelta(seconds=90),
            )
            for name in ("noop", "another-workflow")
        ]
        started = asyncio.get_running_loop().time()
        assert await asyncio.gather(*(handle.result() for handle in handles)) == [
            "success", "success"
        ]
        assert asyncio.get_running_loop().time() - started >= 15
        for handle in handles:
            history = await handle.fetch_history()
            scheduled = [
                event.activity_task_scheduled_event_attributes
                for event in history.events
                if event.HasField("activity_task_scheduled_event_attributes")
            ]
            assert len(scheduled) == 1
            assert scheduled[0].activity_type.name == handle.id.removesuffix(f"-{suffix}")
            assert scheduled[0].start_to_close_timeout.seconds == 60
            completed = next(
                event.activity_task_completed_event_attributes
                for event in history.events
                if event.HasField("activity_task_completed_event_attributes")
            )
            assert await client.data_converter.decode(completed.result.payloads) == ["Success"]

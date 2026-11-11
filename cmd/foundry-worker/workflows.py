from collections.abc import Sequence
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RawValue


@workflow.defn(dynamic=True)
class NoopWorkflow:
    @workflow.run
    async def run(self, args: Sequence[RawValue]) -> str:
        name = workflow.info().workflow_type
        workflow.logger.info("Running workflow workflow_name=%s", name)
        await workflow.execute_activity(
            name, start_to_close_timeout=timedelta(minutes=1)
        )
        return "success"

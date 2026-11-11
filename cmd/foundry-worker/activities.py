import asyncio
from collections.abc import Sequence

from temporalio import activity
from temporalio.common import RawValue


@activity.defn(dynamic=True)
async def noop_activity(args: Sequence[RawValue]) -> str:
    activity.logger.info(
        "Running activity activity_name=%s", activity.info().activity_type
    )
    await asyncio.sleep(15)
    return "Success"

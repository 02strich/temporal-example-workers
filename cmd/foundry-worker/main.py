"""Foundry's invocation handler only wakes the background worker."""

from azure.ai.agentserver.invocations import InvocationAgentServerHost
from starlette.requests import Request
from starlette.responses import JSONResponse

from supervisor import WorkerSupervisor


def create_app(supervisor: WorkerSupervisor | None = None) -> InvocationAgentServerHost:
    supervisor = supervisor or WorkerSupervisor()
    app = InvocationAgentServerHost()
    app.register_pre_shutdown_callback(supervisor.request_shutdown)

    @app.invoke_handler
    async def invoke(_request: Request) -> JSONResponse:
        if not supervisor.start():
            return JSONResponse({"status": "shutting_down"}, status_code=503)
        return JSONResponse({"status": "accepted"}, status_code=202)

    @app.shutdown_handler
    async def shutdown() -> None:
        await supervisor.close()

    return app


if __name__ == "__main__":
    create_app().run()

FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1
WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.13.7 && uv sync --frozen --no-dev --no-install-project

COPY main.py supervisor.py worker.py workflows.py activities.py ./
RUN useradd --create-home --uid 10001 agent
USER agent
EXPOSE 8088
CMD ["/app/.venv/bin/python", "/app/main.py"]

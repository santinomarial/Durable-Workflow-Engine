# syntax=docker/dockerfile:1.8
FROM python:3.12.13-slim-bookworm AS builder

ARG UV_VERSION=0.11.2
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN python -m pip install --no-cache-dir "uv==${UV_VERSION}"
WORKDIR /build
COPY pyproject.toml uv.lock README.md ./
COPY engine ./engine
COPY migrations ./migrations
COPY ui ./ui
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12.13-slim-bookworm AS runtime

LABEL org.opencontainers.image.title="Durable Workflow Engine" \
      org.opencontainers.image.source="https://github.com/santinomarial/Durable-Workflow-Engine" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DWE_LOG_FORMAT=json

RUN groupadd --gid 10001 durable \
    && useradd --uid 10001 --gid durable --no-create-home --home-dir /nonexistent durable

WORKDIR /app
COPY --from=builder --chown=durable:durable /build/.venv ./.venv
COPY --chown=durable:durable examples ./examples

USER 10001:10001
EXPOSE 8000
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=4 \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/health/live', timeout=2).read()"]

CMD ["python", "-m", "uvicorn", "engine.api.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-server-header", "--timeout-graceful-shutdown", "30"]

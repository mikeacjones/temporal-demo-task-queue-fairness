# One image, two processes:
#   API (default):  python -m fairness_demo.api
#   Worker:         python -m fairness_demo.worker
FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY fairness_demo ./fairness_demo
COPY web ./web
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    API_HOST=0.0.0.0
EXPOSE 8080
CMD ["python", "-m", "fairness_demo.api"]

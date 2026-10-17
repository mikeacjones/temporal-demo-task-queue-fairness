"""Runtime settings, read from environment variables only."""

import os

from temporalio.envconfig import ClientConfig


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _per_tenant(name: str, default: str) -> dict[str, float]:
    raw = os.environ.get(name, default)
    pairs = (item.split("=", 1) for item in raw.split(",") if item.strip())
    return {k.strip(): float(v) for k, v in pairs}


# One variable names every Task Queue, so a crashable workspace's injected value
# (<base>-<workspace id>) keeps the API, the uploads and the Workers paired.
TASK_QUEUE = os.environ.get("TASK_QUEUE", "docs")
FIFO_QUEUE = f"{TASK_QUEUE}-fifo"
FAIR_QUEUE = f"{TASK_QUEUE}-fair"
CONTROL_QUEUE = f"{TASK_QUEUE}-control"

ACME_BULK_DOCS = _int("ACME_BULK_DOCS", 2000)
TIERED_DOCS_PER_TENANT = _int("TIERED_DOCS_PER_TENANT", 500)
TRICKLE_DOCS = {k: int(v) for k, v in _per_tenant("TRICKLE_DOCS", "birch=7,cedar=5,dune=6").items()}
TRICKLE_INTERVAL_S = _per_tenant("TRICKLE_INTERVAL_S", "birch=18,cedar=26,dune=22")

DEFAULT_WORKER_SLOTS = _int("WORKER_SLOTS", 8)
MAX_WORKER_SLOTS = 32

API_HOST = os.environ.get("API_HOST", "127.0.0.1")
API_PORT = _int("API_PORT", 8080)
SESSION_SECRET = os.environ.get("SESSION_SECRET", "")
TEMPORAL_UI_URL = os.environ.get("TEMPORAL_UI_URL", "http://localhost:8233")

SIM_MODEL_SECONDS = _float("SIM_MODEL_SECONDS", 1.5)
SIM_TOOL_SECONDS = _float("SIM_TOOL_SECONDS", 0.5)


def client_connect_config() -> dict:
    """Connection settings from TEMPORAL_* env vars, never from a CLI profile file.

    A developer's default profile may point at a real Temporal Cloud namespace.
    """
    config = ClientConfig.load_client_connect_config(disable_file=True)
    config.setdefault("target_host", "localhost:7233")
    config.setdefault("namespace", "default")
    return config

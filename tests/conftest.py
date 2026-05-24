"""Shared pytest configuration for the Weather Backend test suite.

Responsibilities:
- Inject project root and ``functions/`` directory into ``sys.path`` so tests
  can import as ``from functions.x import y`` while ``functions/main.py`` can
  still do its own ``from services.weather_api import ...`` style imports.
- Load ``.env`` from the project root (for local development; CI uses real
  secrets injected as environment variables).
- Auto-skip ``@pytest.mark.integration`` tests when required API keys are not
  available, so a clean CI run never hits the real CWA / NCDR / MONEV APIs.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FUNCTIONS_DIR = PROJECT_ROOT / "functions"

for path in (PROJECT_ROOT, FUNCTIONS_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

load_dotenv(PROJECT_ROOT / ".env")


INTEGRATION_REQUIRED_ENV = (
    "CWA_API_KEY",
    "NCDR_API_KEY",
    "MONEV_API_KEY",
)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip integration tests when secrets are missing.

    Rationale: ``pytest -m unit`` should be possible in any clean environment
    without leaking API calls. If a developer runs ``pytest`` (no marker
    filter) on a machine without ``.env``, integration tests are silently
    skipped rather than failing noisily.
    """
    missing = [name for name in INTEGRATION_REQUIRED_ENV if not os.getenv(name)]
    if not missing:
        return

    skip_marker = pytest.mark.skip(
        reason=f"Integration tests skipped: missing env vars {missing}"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)

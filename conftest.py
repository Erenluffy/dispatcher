"""pytest setup for the dispatcher suite.

``dispatcher.app`` builds a module-level app at import time, exactly as the
Render start command expects.  Give it a syntactically valid (but unreachable)
fleet so importing the package never logs a configuration error in tests.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("WORKER_URLS", "https://unused-worker.example.invalid")
os.environ.setdefault(
    "WORKER_AUTH_TOKEN", "test-only-dispatcher-worker-token-000")

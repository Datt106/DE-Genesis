from __future__ import annotations

import sys
from pathlib import Path


MOCK_API_ROOT = Path(__file__).resolve().parents[1]
if str(MOCK_API_ROOT) not in sys.path:
    sys.path.insert(0, str(MOCK_API_ROOT))

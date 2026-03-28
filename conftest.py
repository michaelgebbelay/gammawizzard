"""Root conftest — ensures project root is on sys.path for all tests."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

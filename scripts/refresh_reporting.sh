#!/usr/bin/env bash
# Quick local reporting refresh.
# Sources .env in repo root, then runs cs_refresh_all.py.
#
# Usage:
#   ./scripts/refresh_reporting.sh              # with .env
#   ./scripts/refresh_reporting.sh --from-ssm   # load from SSM
#   ./scripts/refresh_reporting.sh --skip gw_signal
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a; source .env; set +a
fi

python3 scripts/data/cs_refresh_all.py "$@"

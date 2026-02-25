#!/usr/bin/env bash
# Cron wrapper for sim chain collection.
# Called 4x/day on trading days (Mon-Fri).
#
# Crontab (PST — adjust if daylight saving):
#   31 6  * * 1-5  /Users/mgebremichael/Documents/Gamma/scripts/collect_chain_cron.sh open
#   0  9  * * 1-5  /Users/mgebremichael/Documents/Gamma/scripts/collect_chain_cron.sh mid
#   0  13 * * 1-5  /Users/mgebremichael/Documents/Gamma/scripts/collect_chain_cron.sh close
#   5  13 * * 1-5  /Users/mgebremichael/Documents/Gamma/scripts/collect_chain_cron.sh close5

set -euo pipefail

REPO="/Users/mgebremichael/Documents/Gamma"
VENV="$REPO/venv/bin/python3"
LOG_DIR="$REPO/logs"
mkdir -p "$LOG_DIR"

PHASE="${1:?Usage: $0 <open|mid|close|close5>}"
DATE=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/collect_chain_${DATE}.log"

echo "=== $(date) | phase=$PHASE ===" >> "$LOG_FILE"
"$VENV" -m sim.data.collect_chain "$PHASE" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?
echo "=== exit=$EXIT_CODE ===" >> "$LOG_FILE"
exit $EXIT_CODE

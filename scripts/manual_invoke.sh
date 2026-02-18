#!/usr/bin/env bash
# Manual invoke: triggers the trading Lambda.
# Usage:
#   ./scripts/manual_invoke.sh              # all 3 automated accounts
#   ./scripts/manual_invoke.sh manual       # manual trades from Google Sheet
#   ./scripts/manual_invoke.sh schwab       # single account
set -euo pipefail

FUNC="gamma-trading-TradingFunction-O0rzFPNn3umX"
REGION="us-east-1"

if [ $# -ge 1 ]; then
  ACCOUNTS=("$@")
else
  ACCOUNTS=(schwab tt-ira tt-individual)
fi

for acct in "${ACCOUNTS[@]}"; do
  echo ">>> Invoking $acct ..."
  aws lambda invoke \
    --function-name "$FUNC" \
    --region "$REGION" \
    --payload "{\"account\": \"$acct\"}" \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 300 \
    /tmp/lambda_${acct}.json
  echo "    Response:"
  cat /tmp/lambda_${acct}.json
  echo
done

echo "Done. ${#ACCOUNTS[@]} account(s) invoked."

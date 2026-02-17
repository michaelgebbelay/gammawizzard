#!/usr/bin/env bash
# Manual invoke: triggers the trading Lambda for all 3 accounts.
# Usage: ./scripts/manual_invoke.sh
set -euo pipefail

FUNC="gamma-trading-TradingFunction-O0rzFPNn3umX"
REGION="us-east-1"

for acct in schwab tt-ira tt-individual; do
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

echo "Done. All 3 accounts invoked."

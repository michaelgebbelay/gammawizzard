#!/usr/bin/env bash
# One-time script to populate SSM Parameter Store with trading secrets.
# Usage: ./lambda/seed_ssm.sh
#
# Prerequisites:
#   - AWS CLI v2 installed and configured (aws configure)
#   - Correct AWS region set (e.g. us-east-1)
#
# All parameters are stored as SecureString (encrypted with default KMS key).

set -euo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"

put_param() {
    local name="$1"
    local prompt="$2"
    local value

    # Check if already set
    existing=$(aws ssm get-parameter --name "$name" --query "Parameter.Value" --output text 2>/dev/null || echo "")
    if [ -n "$existing" ] && [ "$existing" != "None" ]; then
        read -rp "  $name already exists. Overwrite? [y/N]: " yn
        case "$yn" in
            [Yy]*) ;;
            *) echo "  Skipped."; return ;;
        esac
    fi

    read -rp "  $prompt: " value
    if [ -z "$value" ]; then
        echo "  Empty value, skipping."
        return
    fi

    aws ssm put-parameter \
        --name "$name" \
        --value "$value" \
        --type SecureString \
        --overwrite \
        --region "$REGION" \
        --no-cli-pager

    echo "  OK: $name"
}

put_param_file() {
    local name="$1"
    local prompt="$2"
    local path

    existing=$(aws ssm get-parameter --name "$name" --query "Parameter.Value" --output text 2>/dev/null || echo "")
    if [ -n "$existing" ] && [ "$existing" != "None" ]; then
        read -rp "  $name already exists. Overwrite? [y/N]: " yn
        case "$yn" in
            [Yy]*) ;;
            *) echo "  Skipped."; return ;;
        esac
    fi

    read -rp "  $prompt (path to file): " path
    if [ -z "$path" ] || [ ! -f "$path" ]; then
        echo "  File not found, skipping."
        return
    fi

    aws ssm put-parameter \
        --name "$name" \
        --value "file://$path" \
        --type SecureString \
        --overwrite \
        --region "$REGION" \
        --no-cli-pager

    echo "  OK: $name"
}

echo "=== Gamma Trading - SSM Parameter Seeding ==="
echo "Region: $REGION"
echo ""

echo "--- Schwab ---"
put_param  "/gamma/schwab/app_key"      "Schwab App Key"
put_param  "/gamma/schwab/app_secret"   "Schwab App Secret"
put_param_file "/gamma/schwab/token_json" "Schwab token JSON"

echo ""
echo "--- TastyTrade ---"
put_param  "/gamma/tt/client_id"        "TT Client ID"
put_param  "/gamma/tt/client_secret"    "TT Client Secret"
put_param_file "/gamma/tt/token_json"   "TT token JSON"

echo ""
echo "--- Shared ---"
put_param  "/gamma/shared/gsheet_id"       "Google Sheet ID"
put_param  "/gamma/shared/google_sa_json"  "Google SA JSON (base64 or raw)"
put_param  "/gamma/shared/gw_email"        "GammaWizard email"
put_param  "/gamma/shared/gw_password"     "GammaWizard password"

echo ""
echo "--- Notifications (optional, press Enter to skip) ---"
put_param  "/gamma/shared/smtp_user"    "SMTP username"
put_param  "/gamma/shared/smtp_pass"    "SMTP password"
put_param  "/gamma/shared/smtp_to"      "Notification email(s)"

echo ""
echo "Done! Verify with:"
echo "  aws ssm get-parameters-by-path --path /gamma/ --recursive --with-decryption --query 'Parameters[].Name'"

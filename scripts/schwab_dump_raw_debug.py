#!/usr/bin/env python3
import os, json, base64
from datetime import datetime, timedelta, timezone
from schwab.auth import client_from_token_file
from pathlib import Path

def _decode_token_to_path():
    token_env = os.environ.get("SCHWAB_TOKEN_JSON", "")
    p = Path("/tmp/schwab_token.json")
    if token_env:
        try:
            dec = base64.b64decode(token_env).decode("utf-8")
            if dec.strip().startswith("{"):
                token_env = dec
        except Exception:
            pass
    if not token_env.strip():
        raise SystemExit("Missing SCHWAB_TOKEN_JSON")
    p.write_text(token_env)
    return str(p)

def main():
    days_back = int((os.environ.get("DAYS_BACK") or "5").strip())
    start_dt = datetime.now(timezone.utc) - timedelta(days=days_back)
    end_dt   = datetime.now(timezone.utc)

    token_path = _decode_token_to_path()
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]

    c = client_from_token_file(token_path, app_key, app_secret)
    r = c.get_account_numbers(); r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]

    outdir = Path("raw_out")
    outdir.mkdir(parents=True, exist_ok=True)

    # ----- Transactions (ledger) -----
    rt = c.get_transactions(
        acct_hash,
        start_date=start_dt,
        end_date=end_dt
        # leave transaction_types=None for a true "everything" dump
    )
    rt.raise_for_status()
    txns = rt.json()
    (outdir / "transactions.json").write_text(json.dumps(txns, indent=2, sort_keys=True))
    print(f"Wrote {len(txns) if isinstance(txns, list) else '1?'} entries to raw_out/transactions.json")

    # ----- Orders (history with fills) -----
    ro = c.get_orders_for_account(
        acct_hash,
        from_entered_datetime=start_dt,
        to_entered_datetime=end_dt
        # You can add: status=c.Order.Status.FILLED for fills-only
    )
    ro.raise_for_status()
    orders = ro.json()
    # Most accounts get an array; sometimes it's a dict keyed by account—persist it exactly as-is.
    (outdir / "orders.json").write_text(json.dumps(orders, indent=2, sort_keys=True))
    print(f"Wrote {'?' if not isinstance(orders, list) else len(orders)} to raw_out/orders.json")

    # OPTIONAL: pick one filled order and dump the full by-id payload for deep inspection
    try:
        from collections.abc import Iterable
        first_order_id = None
        if isinstance(orders, list) and orders:
            first_order_id = orders[0].get("orderId")
        elif isinstance(orders, dict):
            # some shapes: {"accountId": [...orders...]}; scan for first orderId
            def scan(o):
                if isinstance(o, dict):
                    if "orderId" in o: return o["orderId"]
                    for v in o.values():
                        r = scan(v)
                        if r: return r
                elif isinstance(o, Iterable) and not isinstance(o, (str, bytes)):
                    for v in o:
                        r = scan(v)
                        if r: return r
                return None
            first_order_id = scan(orders)
        if first_order_id:
            rdetail = c.get_order(first_order_id, acct_hash); rdetail.raise_for_status()
            (outdir / f"order_{first_order_id}.json").write_text(json.dumps(rdetail.json(), indent=2, sort_keys=True))
            print(f"Wrote raw_out/order_{first_order_id}.json")
    except Exception as e:
        print(f"NOTE: could not dump order-by-id detail: {e}")

if __name__ == "__main__":
    main()

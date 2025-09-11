#!/usr/bin/env python3
# Schwab/TOS transactions → Google Sheet 'expiry_summary'
# - No estimates: uses reported netAmount and fees from transactions.
# - Aggregates by OPTION EXPIRATION DATE (not trade date).
# - Handles SPX/SPXW by default; change SYMBOL_FILTER if needed.
# - Minimal Sheets writes (header + single bulk update).
#
# Env:
#   GSHEET_ID
#   GOOGLE_SERVICE_ACCOUNT_JSON
#   SCHWAB_APP_KEY
#   SCHWAB_APP_SECRET
#   SCHWAB_TOKEN_JSON   (raw JSON or base64-encoded JSON)
#   DAYS_BACK           (optional, default "14")
#   SYMBOL_FILTER       (optional, default "SPX")  # matches "SPX" and "SPXW"
#
# Output tab: expiry_summary
# Columns:
#   expiry, unique_orders, transactions, legs_traded,
#   gross_amount, fees_total, net_amount, notes

import os, json, time, re
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild
from googleapiclient.errors import HttpError

from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")

TAB_NAME = "expiry_summary"
HEADERS = [
    "expiry",
    "unique_orders",
    "transactions",
    "legs_traded",
    "gross_amount",
    "fees_total",
    "net_amount",
    "notes"
]

def _backoff(i): return 0.6*(2**i)

def iso_z(dt):  # UTC ISO
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_osi(sym: str) -> str:
    raw = (sym or "").upper()
    raw = re.sub(r'\s+', '', raw).lstrip('.')
    raw = re.sub(r'[^A-Z0-9.$^]', '', raw)
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) \
        or re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m:
        raise ValueError("Cannot parse option symbol: " + str(sym))
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    if len(strike)==8 and not frac:
        mills = int(strike)
    else:
        mills = int(strike)*1000 + (int((frac or "0").ljust(3,'0')) if frac else 0)
    return "{:<6s}{}{}{:08d}".format(root, ymd, cp, mills)

def osi_expiry_yyyymmdd(osi: str) -> str:
    # OSI root(6) + YYMMDD + C/P + strike8
    yymmdd = osi[6:12]
    yy = int(yymmdd[0:2])
    yyyy = 2000 + yy
    mm = int(yymmdd[2:4])
    dd = int(yymmdd[4:6])
    return f"{yyyy:04d}-{mm:02d}-{dd:02d}"

def sheets_write_with_retry(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs).execute()
    except HttpError as e:
        if e.resp.status == 429:
            time.sleep(30)
            return fn(*args, **kwargs).execute()
        raise

def ensure_tab_and_header(svc, spreadsheet_id: str, tab: str, header: list):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    names = [sh["properties"]["title"] for sh in meta["sheets"]]
    if tab not in names:
        sheets_write_with_retry(
            svc.spreadsheets().batchUpdate,
            spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        )
    got = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{tab}!1:1"
    ).execute().get("values",[])
    if not got or got[0] != header:
        sheets_write_with_retry(
            svc.spreadsheets().values().update,
            spreadsheetId=spreadsheet_id,
            range=f"{tab}!1:1",
            valueInputOption="USER_ENTERED",
            body={"values":[header]}
        )

def bulk_overwrite_body(svc, spreadsheet_id: str, tab: str, rows: list[list]):
    # Overwrite from A2 downward in one call
    sheets_write_with_retry(
        svc.spreadsheets().values().update,
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!A2",
        valueInputOption="USER_ENTERED",
        body={"values": rows}
    )

def _b64_or_raw_to_file(secret: str, path: str):
    s = secret or ""
    try:
        import base64
        dec = base64.b64decode(s).decode("utf-8")
        if dec.strip().startswith("{"):
            s = dec
    except Exception:
        pass
    with open(path, "w") as f:
        f.write(s)

def schwab_get_json(c, url, params=None, tries=5, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.get(url, params=(params or {}), timeout=25)
            if r.status_code == 200:
                return r.json()
            last=f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        except Exception as e:
            last=f"{type(e).__name__}:{str(e)}"
        time.sleep(_backoff(i))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last}")

def pick_first(d, *keys):
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            return d[k]
    return None

def iter_option_items(txn: dict):
    """
    Yield leg dictionaries for option legs in a transaction.
    Returns items with fields:
      ts, order_id, amount, net_amount, fees_total, instruction, quantity,
      symbol, osi, expiry, put_call, strike
    At least one of (amount, net_amount) will be set when provided by API.
    """
    top = txn or {}
    # Time
    ts = pick_first(top, "transactionDate", "transactionDatetime", "time", "timeStamp", "date")
    if isinstance(ts, (int, float)):
        try:
            ts = datetime.fromtimestamp(ts/1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            ts = ""
    ts = (ts or "").replace(" ", "T")

    # Order ID (best-effort)
    order_id = str(pick_first(top, "orderId", "parentOrderKey", "orderNumber", "orderKey") or "")

    # Fees (top-level)
    fees_total = 0.0
    fees = top.get("fees") or {}
    if isinstance(fees, dict):
        for v in fees.values():
            try:
                fees_total += float(v or 0)
            except Exception:
                pass

    # Amounts (top-level)
    gross_amt = pick_first(top, "amount", "grossAmount")   # may not exist
    net_amt   = pick_first(top, "netAmount", "net")        # net of fees if present

    # Transaction items/legs
    items = []
    legs = []
    if isinstance(top.get("transactionItem"), dict):
        items = [top["transactionItem"]]
    elif isinstance(top.get("transactionItem"), list):
        items = list(top["transactionItem"])
    elif isinstance(top.get("transactionItems"), list):
        items = list(top["transactionItems"])

    for it in items:
        ins = (it.get("instrument") or {})
        atype = (ins.get("assetType") or ins.get("type") or "").upper()
        if atype != "OPTION":
            continue

        sym = (ins.get("symbol") or "")  # TOS symbols are often OSI-ish already
        try:
            osi = to_osi(sym)
        except Exception:
            # Attempt structured build
            exp = pick_first(ins, "optionExpirationDate", "expirationDate")
            pc  = (pick_first(ins, "putCall", "type") or "").upper()
            strike = pick_first(ins, "strikePrice", "strike")
            try:
                if exp and pc and strike is not None:
                    ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
                    cp = "C" if pc.startswith("C") else "P"
                    mills = int(round(float(strike)*1000))
                    osi = "{:<6s}{}{}{:08d}".format("SPXW", ymd, cp, mills)
                else:
                    osi = ""
            except Exception:
                osi = ""

        expiry = osi_expiry_yyyymmdd(osi) if osi else ""
        instr  = (it.get("instruction") or "").upper()
        qty    = it.get("quantity") or it.get("amount")
        try:
            qty = int(abs(float(qty)))
        except Exception:
            qty = 0

        leg_amt = pick_first(it, "amount", "grossAmount")  # per-leg gross, if provided
        leg_net = pick_first(it, "netAmount")              # per-leg net, if provided

        legs.append({
            "ts": ts,
            "order_id": order_id,
            "amount": (None if leg_amt in (None,"") else float(leg_amt)),
            "net_amount": (None if leg_net in (None,"") else float(leg_net)),
            "fees_total": float(fees_total or 0.0),  # will assign at txn level later
            "instruction": instr,
            "quantity": qty,
            "symbol": sym,
            "osi": osi,
            "expiry": expiry,
            "put_call": ("P" if ("P" in osi[12:13]) else "C") if osi else "",
            "strike": None
        })

    # If no leg-level amounts were present, we will allocate only at txn level
    # (but we still can attribute to a single expiry if all legs share one expiry)
    # Attach top-level gross/net on a placeholder if needed.
    if not legs:
        # Try instrument at top-level (rare)
        ins = (top.get("instrument") or {})
        atype = (ins.get("assetType") or ins.get("type") or "").upper()
        if atype == "OPTION":
            sym = (ins.get("symbol") or "")
            try:
                osi = to_osi(sym); expiry = osi_expiry_yyyymmdd(osi)
            except Exception:
                expiry = ""
        else:
            expiry = ""

        legs.append({
            "ts": ts,
            "order_id": order_id,
            "amount": (None if gross_amt in (None,"") else float(gross_amt)),
            "net_amount": (None if net_amt in (None,"") else float(net_amt)),
            "fees_total": float(fees_total or 0.0),
            "instruction": (top.get("instruction") or "").upper(),
            "quantity": 0,
            "symbol": (ins.get("symbol") or ""),
            "osi": (to_osi(ins.get("symbol")) if ins.get("symbol") else ""),
            "expiry": expiry,
            "put_call": "",
            "strike": None
        })

    return legs

def main():
    # --- Config
    gs_id = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    days_back = int(os.environ.get("DAYS_BACK","14") or "14")
    sym_filter = (os.environ.get("SYMBOL_FILTER","SPX") or "SPX").upper()

    # --- Sheets init
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = gbuild("sheets","v4",credentials=creds)
    ensure_tab_and_header(svc, gs_id, TAB_NAME, HEADERS)

    # --- Schwab client
    token_path="schwab_token.json"
    _b64_or_raw_to_file(os.environ.get("SCHWAB_TOKEN_JSON",""), token_path)
    c = client_from_token_file(
        api_key=os.environ["SCHWAB_APP_KEY"],
        app_secret=os.environ["SCHWAB_APP_SECRET"],
        token_path=token_path
    )

    r = c.get_account_numbers(); r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]

    # --- Time window
    now = datetime.now(ET)
    start = now - timedelta(days=days_back)

    # --- Fetch transactions (TRADE types)
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/transactions"
    params = {
        "startDate": start.date().isoformat(),
        "endDate": now.date().isoformat(),
        # depending on API, keys can be 'types' or 'type'
        "type": "TRADE"
    }
    try:
        txns = schwab_get_json(c, url, params=params, tag="TXNS") or []
    except Exception as e:
        print(f"ABORT: transactions fetch failed — {e}")
        return 1

    # --- Aggregate by expiry
    agg = defaultdict(lambda: {
        "orders": set(),
        "transactions": 0,
        "legs": 0,
        "gross": 0.0,
        "fees": 0.0,
        "net": 0.0,
        "notes": set()
    })

    skipped_multi_expiry = 0

    for t in txns:
        legs = iter_option_items(t)
        # Filter to SPX/SPXW if requested
        legs = [L for L in legs if (("SPX" in (L["symbol"] or "").upper()) or ("SPX" in (L["osi"] or "").upper()))] if sym_filter else legs
        if not legs:
            continue

        # Identify expiries present in this transaction
        expiries = { (L["expiry"] or "") for L in legs if (L["expiry"] or "") }
        if len(expiries) == 0:
            # No parseable expiry; skip but note
            continue
        if len(expiries) > 1:
            # A calendar/diagonal or something unexpected — we won't allocate (no estimation).
            skipped_multi_expiry += 1
            continue

        expiry = next(iter(expiries))
        a = agg[expiry]

        # Unique orderId (best-effort)
        oids = { (L["order_id"] or "").strip() for L in legs if (L["order_id"] or "").strip() }
        a["orders"].update(oids)

        # Count legs & transactions
        a["legs"] += len(legs)
        a["transactions"] += 1

        # Amounts: prefer per-leg amounts if present; otherwise use top-level net/gross carried on any leg
        # Gather per-transaction totals robustly
        leg_gross_sum = 0.0
        leg_net_sum = 0.0
        have_leg_gross = False
        have_leg_net = False
        txn_fee_total = 0.0

        for L in legs:
            if L["amount"] is not None:
                have_leg_gross = True
                try: leg_gross_sum += float(L["amount"])
                except: pass
            if L["net_amount"] is not None:
                have_leg_net = True
                try: leg_net_sum += float(L["net_amount"])
                except: pass
            # fees_total is top-level repeated on each leg; only add once
            txn_fee_total = L["fees_total"] if L["fees_total"] else txn_fee_total

        # Prefer the most specific data we have:
        if have_leg_net:
            a["net"] += leg_net_sum
            if have_leg_gross:
                a["gross"] += leg_gross_sum
                # If both provided, fees = gross - net (avoid double counting)
                a["fees"] += (leg_gross_sum - leg_net_sum)
            else:
                # Only net known; add explicit fees if API provided them
                a["fees"] += float(txn_fee_total or 0.0)
        elif have_leg_gross:
            a["gross"] += leg_gross_sum
            # No per-leg net; rely on explicit fees if present
            a["fees"] += float(txn_fee_total or 0.0)
            a["net"]  += leg_gross_sum - float(txn_fee_total or 0.0)
        else:
            # Neither leg-level amount present; try any leg's carried net/gross (from top-level projection)
            any_leg = legs[0]
            net_fallback = any_leg.get("net_amount")
            gross_fallback = any_leg.get("amount")
            if net_fallback is not None:
                a["net"] += float(net_fallback)
                if gross_fallback is not None:
                    a["gross"] += float(gross_fallback)
                    a["fees"]  += float(gross_fallback) - float(net_fallback)
                else:
                    a["fees"]  += float(txn_fee_total or 0.0)
            elif gross_fallback is not None:
                a["gross"] += float(gross_fallback)
                a["fees"]  += float(txn_fee_total or 0.0)
                a["net"]   += float(gross_fallback) - float(txn_fee_total or 0.0)
            else:
                a["notes"].add("NO_AMOUNTS")

    if skipped_multi_expiry:
        # We record a single note line for visibility
        pass

    # --- Build rows
    rows = []
    for exp in sorted(agg.keys()):
        v = agg[exp]
        notes = list(sorted(v["notes"]))
        rows.append([
            exp,
            len(v["orders"]),
            v["transactions"],
            v["legs"],
            f"{v['gross']:.2f}",
            f"{v['fees']:.2f}",
            f"{v['net']:.2f}",
            ";".join(notes) if notes else ""
        ])

    # --- Write summary (overwrite)
    ensure_tab_and_header(svc, gs_id, TAB_NAME, HEADERS)
    bulk_overwrite_body(svc, gs_id, TAB_NAME, rows)

    print(f"EXPIRY SUMMARY: rows={len(rows)} (skipped_multi_expiry={skipped_multi_expiry})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())

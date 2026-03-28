"""Import and query strategy signal rows for reporting."""

from __future__ import annotations

import csv
import json
import uuid
from pathlib import Path

from reporting.db import execute, get_connection, init_schema


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SIGNAL_GLOB = "sim/data/leo_stable*.csv"


def _find_latest_signal_csv() -> Path | None:
    files = sorted(REPO_ROOT.glob(DEFAULT_SIGNAL_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _s(value) -> str:
    return (value or "").strip()


def _f(value):
    text = _s(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _i(value):
    text = _s(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _signal_id(strategy: str, source: str, trade_date: str, expiry_date: str) -> str:
    return f"{strategy}:{source}:{trade_date}:{expiry_date or ''}"


def import_constantstable_signal_rows(
    csv_path: str | Path | None = None,
    *,
    source: str = "leo_csv",
    con=None,
) -> dict:
    """Import ConstantStable daily signal rows from the latest Leo/GW CSV."""
    if con is None:
        con = get_connection()
        init_schema(con)

    if csv_path is None:
        path = _find_latest_signal_csv()
        if path is None:
            return {"source_path": "", "rows_read": 0, "inserted": 0, "updated": 0}
    else:
        path = Path(csv_path)
        if not path.exists():
            return {"source_path": str(path), "rows_read": 0, "inserted": 0, "updated": 0}

    rows_read = inserted = updated = 0
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trade_date = _s(row.get("Date"))
            if not trade_date:
                continue
            expiry_date = _s(row.get("TDate"))
            signal_id = _signal_id("constantstable", source, trade_date, expiry_date)

            exists = con.execute(
                "SELECT 1 FROM strategy_signal_rows WHERE signal_id = ?",
                [signal_id],
            ).fetchone()

            payload = json.dumps(row, separators=(",", ":"), sort_keys=True)
            params = [
                signal_id,
                "constantstable",
                source,
                trade_date,
                expiry_date or None,
                _f(row.get("SPX")),
                _f(row.get("Forward")),
                _f(row.get("VIX")),
                _f(row.get("VixOne")),
                _f(row.get("Limit")),
                _f(row.get("CLimit")),
                _f(row.get("Put")),
                _f(row.get("Call")),
                _f(row.get("LeftGo")),
                _f(row.get("RightGo")),
                _f(row.get("LImp")),
                _f(row.get("RImp")),
                _f(row.get("LReturn")),
                _f(row.get("RReturn")),
                _f(row.get("FP")),
                _f(row.get("RV")),
                _f(row.get("RV5")),
                _f(row.get("RV10")),
                _f(row.get("RV20")),
                _f(row.get("R")),
                _f(row.get("AR")),
                _f(row.get("AR2")),
                _f(row.get("AR3")),
                _f(row.get("TX")),
                _i(row.get("Y")),
                _i(row.get("M")),
                payload,
            ]

            if exists:
                execute(
                    """UPDATE strategy_signal_rows
                       SET spot = ?, forward = ?, vix = ?, vix_one = ?,
                           put_strike = ?, call_strike = ?, put_price = ?, call_price = ?,
                           left_go = ?, right_go = ?, l_imp = ?, r_imp = ?,
                           l_return = ?, r_return = ?, fp = ?, rv = ?, rv5 = ?, rv10 = ?, rv20 = ?,
                           r = ?, ar = ?, ar2 = ?, ar3 = ?, tx = ?, year_num = ?, month_num = ?,
                           raw_payload = ?
                       WHERE signal_id = ?""",
                    params[5:] + [signal_id],
                    con=con,
                )
                updated += 1
            else:
                execute(
                    """INSERT INTO strategy_signal_rows
                       (signal_id, strategy, source, trade_date, expiry_date,
                        spot, forward, vix, vix_one,
                        put_strike, call_strike, put_price, call_price,
                        left_go, right_go, l_imp, r_imp, l_return, r_return, fp,
                        rv, rv5, rv10, rv20, r, ar, ar2, ar3, tx, year_num, month_num, raw_payload)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    params,
                    con=con,
                )
                inserted += 1
            rows_read += 1

    return {
        "source_path": str(path),
        "rows_read": rows_read,
        "inserted": inserted,
        "updated": updated,
    }

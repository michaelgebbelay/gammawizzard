#!/usr/bin/env python3
"""Build sw_orig_orders from sw_txn_raw: detect iron condor opens in
the 16:00-16:20 ET window on the business day before expiry and write
[exp_primary, side, contracts] per expiry."""

import os, sys, re
from typing import Any, List, Dict, Tuple, Optional
from datetime import datetime, date, time, timedelta

# -- path setup (find repo-level scripts/ from TT/data/) --
def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        scripts = os.path.join(cur, "scripts")
        if os.path.isdir(scripts):
            if scripts not in sys.path:
                sys.path.append(scripts)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent

_add_scripts_root()

from lib.sheets import sheets_client, ensure_tab, overwrite_rows, get_values
from lib.parsing import ET, safe_float, iso_fix

RAW_TAB = "sw_txn_raw"
OUT_TAB = "sw_orig_orders"
OUT_HEADERS = ["exp_primary", "side", "contracts"]


def _idx(hdr: List[str], col: str) -> int:
    try:
        return hdr.index(col)
    except ValueError:
        return -1


def _dt_et(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(iso_fix(s))
        return dt.astimezone(ET)
    except Exception:
        return None


def _prev_bday(d: date, n: int) -> date:
    cur, step = d, 0
    while step < n:
        cur = cur - timedelta(days=1)
        if cur.weekday() < 5:
            step += 1
    return cur


def _build_orig(svc, sid):
    vals = get_values(svc, sid, f"{RAW_TAB}!A1:R")
    if not vals:
        ensure_tab(svc, sid, OUT_TAB, OUT_HEADERS)
        overwrite_rows(svc, sid, OUT_TAB, OUT_HEADERS, [])
        print("sw_orig_orders: no raw data.")
        return

    hdr = vals[0]
    i_ts = _idx(hdr, "ts")
    i_ledger = _idx(hdr, "ledger_id")
    i_exp = _idx(hdr, "exp_primary")
    i_pc = _idx(hdr, "put_call")
    i_strk = _idx(hdr, "strike")
    i_qty = _idx(hdr, "quantity")
    i_amt = _idx(hdr, "amount")
    i_net = _idx(hdr, "net_amount")
    rows = [r + [""] * (len(hdr) - len(r)) for r in vals[1:]]

    # Window config
    T0 = os.environ.get("ORIG_ET_START", "16:00")
    T1 = os.environ.get("ORIG_ET_END", "16:20")

    def _hhmm(s):
        try:
            h, m = [int(x) for x in s.split(":")]
            return time(hour=h, minute=m, tzinfo=ET)
        except Exception:
            return time(16, 0, tzinfo=ET)

    W0, W1 = _hhmm(T0), _hhmm(T1)
    D_BEFORE = int(os.environ.get("ORIG_DAYS_BEFORE", "1") or "1")

    # Collect candidate ledgers per expiry within window
    exps = sorted(
        {(r[i_exp] or "").strip() for r in rows if i_exp >= 0 and r[i_exp]},
        key=lambda s: s,
    )
    out = []
    for e in exps:
        try:
            exp_d = date.fromisoformat(e[:10])
        except Exception:
            continue
        orig_d = _prev_bday(exp_d, D_BEFORE)
        win_start = datetime.combine(orig_d, W0).astimezone(ET)
        win_end = datetime.combine(orig_d, W1).astimezone(ET)

        per_ledger: Dict[str, List[List[Any]]] = {}
        for r in rows:
            if (r[i_exp] or "").strip() != e:
                continue
            lid = (r[i_ledger] or "").strip()
            if not lid:
                continue
            dt = _dt_et(r[i_ts]) if i_ts >= 0 else None
            if not dt or not (win_start <= dt <= win_end):
                continue
            per_ledger.setdefault(lid, []).append(r)

        if not per_ledger:
            continue

        # Score ledgers, compute units and side
        choices: List[Tuple[int, int, datetime, str, Dict[str, Any]]] = []
        for lid, legs in per_ledger.items():
            put_strikes: Dict[float, float] = {}
            call_strikes: Dict[float, float] = {}
            latest = None
            amt_sum = 0.0
            have_amt = True
            net = None
            for r in legs:
                pc = (r[i_pc] or "").strip().upper() if i_pc >= 0 else ""
                s = safe_float(r[i_strk]) if i_strk >= 0 else None
                q = safe_float(r[i_qty]) if i_qty >= 0 else None
                a = safe_float(r[i_amt]) if i_amt >= 0 else None
                ts_dt = _dt_et(r[i_ts]) if i_ts >= 0 else None
                if latest is None or (ts_dt and ts_dt > latest):
                    latest = ts_dt
                if a is None:
                    have_amt = False
                else:
                    amt_sum += a
                if i_net >= 0 and net is None and r[i_net] not in ("", None):
                    net = safe_float(r[i_net])
                if pc and s is not None and q is not None:
                    d = (
                        put_strikes
                        if pc == "PUT"
                        else call_strikes if pc == "CALL" else None
                    )
                    if d is not None:
                        d[s] = d.get(s, 0.0) + q

            if len(put_strikes) < 2 or len(call_strikes) < 2:
                continue

            def top2_units(dmap):
                pairs = sorted(
                    ((abs(q), s) for s, q in dmap.items()), reverse=True
                )
                if not pairs:
                    return []
                top = pairs[:2] if len(pairs) >= 2 else pairs * 2
                return [abs(dmap[top[0][1]]), abs(dmap[top[1][1]])]

            q_puts = top2_units(put_strikes)
            q_calls = top2_units(call_strikes)
            if not q_puts or not q_calls:
                continue
            units = int(round(min(q_puts + q_calls)))
            if units <= 0:
                continue

            if have_amt:
                side = "short" if (amt_sum < 0) else "long"
            elif net is not None:
                side = "short" if (net > 0) else "long"
            else:
                side = "short"

            score = 1
            choices.append(
                (
                    score,
                    units,
                    latest or datetime.min.replace(tzinfo=ET),
                    lid,
                    {"e": e, "side": side, "units": units},
                )
            )

        if not choices:
            continue
        choices.sort(key=lambda x: (x[0], x[1], x[2]))
        best = choices[-1][4]
        out.append([best["e"], best["side"], str(int(best["units"]))])

    out.sort(key=lambda r: r[0])
    ensure_tab(svc, sid, OUT_TAB, OUT_HEADERS)
    overwrite_rows(svc, sid, OUT_TAB, OUT_HEADERS, out)
    print(f"sw_orig_orders: wrote {len(out)} rows (exp_primary|side|contracts).")


def main() -> int:
    svc, sid = sheets_client()
    _build_orig(svc, sid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

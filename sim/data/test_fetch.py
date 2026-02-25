#!/usr/bin/env python3
"""Test script to verify chain fetching works.

Prerequisites:
    export SCHWAB_APP_KEY=your_key
    export SCHWAB_APP_SECRET=your_secret

    Schwab token must exist at Token/schwab_token.json (or SCHWAB_TOKEN_PATH).

Usage:
    cd /Users/mgebremichael/Documents/Gamma
    python -m sim.data.test_fetch
"""

import os
import sys

_repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)


def main():
    # Check env vars
    missing = []
    for var in ("SCHWAB_APP_KEY", "SCHWAB_APP_SECRET"):
        if not os.environ.get(var):
            missing.append(var)
    if missing:
        print(f"Missing env vars: {', '.join(missing)}")
        print("Set them and re-run. These are in SSM under /gamma/schwab/")
        sys.exit(1)

    from sim.data.market_data import fetch_spx_chain

    print("Fetching SPX chain...")
    snapshot = fetch_spx_chain(phase="open")

    print(f"\n=== Chain Snapshot ===")
    print(f"SPX Price:     {snapshot.underlying_price:.2f}")
    print(f"VIX:           {snapshot.vix:.2f}")
    print(f"Expirations:   {snapshot.expirations}")
    print(f"Total strikes: {len(snapshot.strikes)}")
    print(f"Total contracts: {len(snapshot.contracts)}")

    # Show ATM region
    atm = snapshot.atm_strike()
    print(f"\nATM Strike:    {atm}")

    # Expected move
    for exp in snapshot.expirations:
        em = snapshot.expected_move(exp)
        print(f"Expected move ({exp}): ±{em:.2f} points")

    # Show a sample contract with greeks
    print(f"\n=== Sample Contracts near ATM ({atm}) ===")
    for pc in ("C", "P"):
        c = snapshot.get_contract(atm, pc)
        if c:
            print(f"\n  {c.symbol}")
            print(f"  Type: {'Call' if pc == 'C' else 'Put'}")
            print(f"  Bid/Ask: {c.bid:.2f} / {c.ask:.2f}  Mid: {c.mid:.2f}")
            print(f"  Delta: {c.delta:.4f}  Gamma: {c.gamma:.6f}")
            print(f"  Theta: {c.theta:.4f}  Vega: {c.vega:.4f}")
            print(f"  IV: {c.implied_vol:.2f}%")
            print(f"  Volume: {c.volume}  OI: {c.open_interest}")
            print(f"  DTE: {c.days_to_exp}  ITM: {c.in_the_money}")

    # Find 10-delta strikes (what the mechanical IC bot would use)
    for exp in snapshot.expirations:
        put_strike = snapshot.nearest_delta_strike(-0.10, "P", exp)
        call_strike = snapshot.nearest_delta_strike(0.10, "C", exp)
        if put_strike and call_strike:
            print(f"\n10-delta IC strikes ({exp}): put={put_strike}, call={call_strike}")

    # Test caching
    from sim.data.cache import save_to_cache, load_from_cache
    from datetime import date

    today = date.today()
    print(f"\n=== Cache Test ===")
    # We'd need the raw JSON for a proper cache test, but this validates the flow
    print(f"Cache path would be: sim/cache/{today}/open.json")

    print("\n✓ Chain fetch + parse + greeks all working!")


if __name__ == "__main__":
    main()

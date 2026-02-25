"""Backwards-compatibility shim — canonical module is gw_client.py."""
from sim.data.gw_client import (  # noqa: F401
    fetch_gw_data,
    fetch_gw_market_data,
    gate_gw_for_window,
    load_gw_data,
    save_gw_data,
)

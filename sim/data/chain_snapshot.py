"""Normalized dataclasses for SPX options chain data and PM-settled filtering."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional


@dataclass(frozen=True)
class OptionContract:
    """A single option contract with pricing and greeks."""
    symbol: str             # OSI-style symbol
    strike: float
    expiration: date
    put_call: str           # "C" or "P"
    bid: float
    ask: float
    last: float
    mark: float             # (bid+ask)/2
    volume: int
    open_interest: int
    implied_vol: float
    delta: float
    gamma: float
    theta: float
    vega: float
    rho: float
    days_to_exp: int
    in_the_money: bool

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


@dataclass
class ChainSnapshot:
    """A complete options chain snapshot at a point in time."""
    timestamp: datetime
    phase: str                  # "open", "mid", "close"
    underlying_price: float
    underlying_symbol: str      # e.g. "$SPX"
    vix: float
    contracts: Dict[str, OptionContract] = field(default_factory=dict)  # keyed by symbol
    expirations: List[date] = field(default_factory=list)
    strikes: List[float] = field(default_factory=list)
    # OHLC context (populated by TT fetcher)
    spx_open: float = 0.0
    spx_high: float = 0.0
    spx_low: float = 0.0
    spx_prev_close: float = 0.0

    def calls(self, expiration: Optional[date] = None) -> List[OptionContract]:
        """All call contracts, optionally filtered by expiration."""
        return [c for c in self.contracts.values()
                if c.put_call == "C" and (expiration is None or c.expiration == expiration)]

    def puts(self, expiration: Optional[date] = None) -> List[OptionContract]:
        """All put contracts, optionally filtered by expiration."""
        return [c for c in self.contracts.values()
                if c.put_call == "P" and (expiration is None or c.expiration == expiration)]

    def get_contract(self, strike: float, put_call: str,
                     expiration: Optional[date] = None) -> Optional[OptionContract]:
        """Find a specific contract by strike and type."""
        for c in self.contracts.values():
            if (c.strike == strike and c.put_call == put_call
                    and (expiration is None or c.expiration == expiration)):
                return c
        return None

    def atm_strike(self) -> float:
        """Nearest strike to the underlying price. Ties go to the lower strike."""
        if not self.strikes:
            raise ValueError("No strikes available")
        return min(self.strikes, key=lambda s: (abs(s - self.underlying_price), s))

    def nearest_delta_strike(self, target_delta: float, put_call: str,
                             expiration: Optional[date] = None) -> Optional[float]:
        """Find the strike whose delta is closest to target_delta.

        For puts, target_delta should be negative (e.g., -0.10).
        For calls, target_delta should be positive (e.g., 0.10).
        """
        contracts = self.calls(expiration) if put_call == "C" else self.puts(expiration)
        if not contracts:
            return None
        best = min(contracts, key=lambda c: abs(c.delta - target_delta))
        return best.strike

    def expected_move(self, expiration: Optional[date] = None) -> float:
        """Expected move (±1σ) from the ATM straddle price."""
        atm = self.atm_strike()
        call = self.get_contract(atm, "C", expiration)
        put = self.get_contract(atm, "P", expiration)
        if call is None or put is None:
            return 0.0
        return call.mid + put.mid


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _safe_int(val, default: int = 0) -> int:
    if val is None:
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _is_pm_settled(exp_key: str, contracts: dict) -> bool:
    """Determine if an expiration group is PM-settled (SPXW).

    Schwab chain response nests contracts under expiration keys like
    "2026-02-27:5" where the number after the colon is DTE.

    PM-settled identification:
    - Check symbol root for 'SPXW' (weekly/daily PM-settled)
    - If root is 'SPX' (no W), check settlement type field
    - Monthly 3rd-Friday expirations with 'SPX' root are AM-settled → exclude
    """
    # Grab the first contract to inspect its symbol
    for strike_key, strike_data in contracts.items():
        if isinstance(strike_data, list) and len(strike_data) > 0:
            contract = strike_data[0]
            symbol = contract.get("symbol", "")
            # SPXW root → PM-settled
            if "SPXW" in symbol.upper():
                return True
            # Check settlementType field if present
            stype = contract.get("settlementType", "")
            if stype.upper() == "P":
                return True
            if stype.upper() == "A":
                return False
            # If no settlement type, check if it's a 3rd Friday (AM-settled monthly)
            try:
                exp_date = date.fromisoformat(exp_key.split(":")[0])
                # 3rd Friday: 15 <= day <= 21 and weekday == 4 (Friday)
                if 15 <= exp_date.day <= 21 and exp_date.weekday() == 4:
                    # Likely AM-settled monthly — exclude unless symbol says SPXW
                    return False
            except (ValueError, IndexError):
                pass
            # Default: assume PM-settled for non-monthly SPX
            return True
    return False


def _parse_exp_date(exp_key: str) -> date:
    """Parse expiration date from Schwab chain key like '2026-02-27:5'."""
    return date.fromisoformat(exp_key.split(":")[0])


def _parse_dte(exp_key: str) -> int:
    """Parse DTE from Schwab chain key like '2026-02-27:5'."""
    parts = exp_key.split(":")
    if len(parts) >= 2:
        return _safe_int(parts[1], 0)
    return 0


def parse_schwab_chain(raw: dict, phase: str, vix: float = 0.0) -> ChainSnapshot:
    """Parse a Schwab get_option_chain() JSON response into a ChainSnapshot.

    Applies PM-settled (SPXW) filter. Only PM-settled expirations are included.

    Args:
        raw: The raw JSON response from Schwab's get_option_chain()
        phase: "open", "mid", or "close"
        vix: Current VIX level (fetched separately)

    Returns:
        ChainSnapshot with only PM-settled contracts
    """
    underlying = raw.get("underlyingPrice") or raw.get("underlying", {}).get("last", 0.0)
    underlying = _safe_float(underlying)
    symbol = raw.get("symbol", "$SPX")

    contracts: Dict[str, OptionContract] = {}
    expirations_set: set[date] = set()
    strikes_set: set[float] = set()

    for side, put_call in [("callExpDateMap", "C"), ("putExpDateMap", "P")]:
        exp_map = raw.get(side, {})
        for exp_key, strikes_data in exp_map.items():
            if not _is_pm_settled(exp_key, strikes_data):
                continue

            exp_date = _parse_exp_date(exp_key)
            dte = _parse_dte(exp_key)
            expirations_set.add(exp_date)

            for strike_key, contract_list in strikes_data.items():
                if not isinstance(contract_list, list) or len(contract_list) == 0:
                    continue
                c = contract_list[0]
                strike = _safe_float(strike_key)
                strikes_set.add(strike)

                sym = c.get("symbol", "")
                bid = _safe_float(c.get("bid"))
                ask = _safe_float(c.get("ask"))

                oc = OptionContract(
                    symbol=sym,
                    strike=strike,
                    expiration=exp_date,
                    put_call=put_call,
                    bid=bid,
                    ask=ask,
                    last=_safe_float(c.get("last")),
                    mark=_safe_float(c.get("mark", (bid + ask) / 2.0 if bid and ask else 0.0)),
                    volume=_safe_int(c.get("totalVolume")),
                    open_interest=_safe_int(c.get("openInterest")),
                    implied_vol=_safe_float(c.get("volatility")),
                    delta=_safe_float(c.get("delta")),
                    gamma=_safe_float(c.get("gamma")),
                    theta=_safe_float(c.get("theta")),
                    vega=_safe_float(c.get("vega")),
                    rho=_safe_float(c.get("rho")),
                    days_to_exp=dte,
                    in_the_money=bool(c.get("inTheMoney", False)),
                )
                contracts[sym] = oc

    return ChainSnapshot(
        timestamp=datetime.utcnow(),
        phase=phase,
        underlying_price=underlying,
        underlying_symbol=symbol,
        vix=vix,
        contracts=contracts,
        expirations=sorted(expirations_set),
        strikes=sorted(strikes_set),
    )


def parse_tt_chain(raw: dict) -> ChainSnapshot:
    """Parse a TastyTrade chain dict (from tt_market_data.fetch_tt_spx_chain) into a ChainSnapshot.

    Args:
        raw: The normalized dict from fetch_tt_spx_chain().

    Returns:
        ChainSnapshot with all contracts.
    """
    phase = raw.get("_phase", "open")
    vix = _safe_float(raw.get("_vix", 0.0))
    underlying = _safe_float(raw.get("underlying_price", 0.0))
    exp_str = raw.get("expiration", "")

    contracts_raw = raw.get("contracts", {})
    contracts: Dict[str, OptionContract] = {}
    expirations_set: set[date] = set()
    strikes_set: set[float] = set()

    exp_date = date.fromisoformat(exp_str) if exp_str else date.today()
    expirations_set.add(exp_date)

    for sym, c in contracts_raw.items():
        strike = _safe_float(c.get("strike"))
        strikes_set.add(strike)
        bid = _safe_float(c.get("bid"))
        ask = _safe_float(c.get("ask"))

        oc = OptionContract(
            symbol=sym,
            strike=strike,
            expiration=exp_date,
            put_call=c.get("put_call", "C"),
            bid=bid,
            ask=ask,
            last=_safe_float(c.get("last")),
            mark=_safe_float(c.get("mark", (bid + ask) / 2.0 if bid and ask else 0.0)),
            volume=_safe_int(c.get("volume")),
            open_interest=_safe_int(c.get("open_interest")),
            implied_vol=_safe_float(c.get("implied_vol")),
            delta=_safe_float(c.get("delta")),
            gamma=_safe_float(c.get("gamma")),
            theta=_safe_float(c.get("theta")),
            vega=_safe_float(c.get("vega")),
            rho=_safe_float(c.get("rho")),
            days_to_exp=_safe_int(c.get("days_to_exp", 1)),
            in_the_money=bool(c.get("in_the_money", False)),
        )
        contracts[sym] = oc

    ts_str = raw.get("_timestamp", "")
    try:
        ts = datetime.fromisoformat(ts_str) if ts_str else datetime.utcnow()
    except ValueError:
        ts = datetime.utcnow()

    return ChainSnapshot(
        timestamp=ts,
        phase=phase,
        underlying_price=underlying,
        underlying_symbol="$SPX",
        vix=vix,
        contracts=contracts,
        expirations=sorted(expirations_set),
        strikes=sorted(strikes_set),
        spx_open=_safe_float(raw.get("spx_open", 0.0)),
        spx_high=_safe_float(raw.get("spx_high", 0.0)),
        spx_low=_safe_float(raw.get("spx_low", 0.0)),
        spx_prev_close=_safe_float(raw.get("spx_prev_close", 0.0)),
    )

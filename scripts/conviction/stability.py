"""
Stability / "stable compounder" metrics.

Different question from the gainers screen: instead of "what gapped today,"
ask "what's been grinding up in a near-straight line, doesn't dip when SPX
dips, and isn't a wild ride?" Built for the user who'd rather own the quiet
compounder (LLY, COST, MA pre-2025) than the post-earnings gap.

Per ticker, we compute:
    ret_12m            12-month total return
    vol_252            annualized stdev of daily log returns (252-day)
    smoothness         ret_252 / vol_252 (rough Sharpe, no rf subtracted)
    mdd_252            max drawdown over the last 252 days (negative number)
    calmar             ret_12m / |mdd_252| — return per unit of pain
    r_sq_126           R² of log-close vs trading-day index (last 126 days).
                       1.0 = perfect line; <0.85 = bumpy
    corr_spx           daily-return correlation with SPX (252-day)
    beta_spx           daily-return beta vs SPX
    down_capture       average daily return on days SPX was <= -1%
                       (positive = name went UP while market went down)
    above_200d         current close vs SMA-200 (uptrend confirm)

A "stable compounder" looks like: ret_12m > 0, mdd_252 small (e.g. > -15%),
r_sq_126 > 0.85, smoothness > 1.5, corr_spx low (e.g. < 0.5), and
down_capture >= 0.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass
class StabilityFactors:
    ticker: str
    last_close: float
    n_bars: int
    ret_12m: float | None = None
    vol_252: float | None = None
    smoothness: float | None = None
    mdd_252: float | None = None
    calmar: float | None = None
    r_sq_126: float | None = None
    corr_spx: float | None = None
    beta_spx: float | None = None
    down_capture: float | None = None
    above_200d: float | None = None
    dollar_vol_20d: float | None = None

    def above_200d_or_zero(self) -> bool:
        """Whether last_close > SMA200. Treat None as 'unknown / fail'."""
        return bool(self.above_200d is not None and self.above_200d > 0)
    # Ride/eject trend-health signals
    above_21d_ema: bool | None = None      # short-term trend intact
    above_50d_sma: bool | None = None      # medium-term trend intact
    pct_from_52w_high: float | None = None # negative = below high (closer to 0 = healthier entry)
    pct_from_recent_high_60d: float | None = None  # current drawdown vs trailing 60d peak
    days_since_52w_high: int | None = None # 0 = at high; >40 = trend losing breath
    recent_60d_ret: float | None = None    # last 60 trading days return
    trend_status: str | None = None        # INTACT | PULLBACK | WARNING | BROKEN
    composite: float | None = None         # populated by rank_universe
    # Volatility-regime fields used by Path P (pullback-continuation entry)
    atr_pct: float | None = None           # 20d ATR / close — name's typical daily swing
    vol_20d: float | None = None           # annualized stdev of last 20 daily log returns
    vol_60d: float | None = None           # annualized stdev of last 60 daily log returns
    coil_ratio: float | None = None        # vol_20d / vol_60d — <1 coiling, >1 expanding


def _max_drawdown(close: pd.Series) -> float | None:
    if close.empty:
        return None
    running_max = close.cummax()
    dd = close / running_max - 1.0
    return float(dd.min())


def _r_squared_linear(log_close: pd.Series) -> float | None:
    if len(log_close) < 30:
        return None
    x = np.arange(len(log_close), dtype=float)
    y = log_close.values
    if not np.isfinite(y).all():
        return None
    # least squares linear fit; R² = 1 - SS_res / SS_tot
    coeffs = np.polyfit(x, y, 1)
    yhat = np.polyval(coeffs, x)
    ss_res = float(np.sum((y - yhat) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    if ss_tot == 0:
        return None
    return max(0.0, 1.0 - ss_res / ss_tot)


def _safe_log_returns(close: pd.Series) -> pd.Series:
    return np.log(close / close.shift(1)).dropna()


def compute_stability_factors(
    ticker: str,
    bars: pd.DataFrame,
    spx_log_returns: pd.Series | None = None,
) -> StabilityFactors | None:
    """Returns None if there isn't enough history."""
    if bars is None or bars.empty or len(bars) < 200:
        return None

    df = bars.sort_values("date").reset_index(drop=True)
    close = df["close"].astype(float)
    volume = df["volume"].astype(float)
    last = float(close.iloc[-1])

    factors = StabilityFactors(ticker=ticker.upper(), last_close=last, n_bars=len(df))

    # 12-month return
    if len(close) > 252:
        c0 = float(close.iloc[-253])
        if c0 > 0:
            factors.ret_12m = last / c0 - 1.0

    log_ret = _safe_log_returns(close)
    if len(log_ret) >= 60:
        factors.vol_252 = float(log_ret.iloc[-252:].std() * np.sqrt(252))
        if factors.ret_12m is not None and factors.vol_252 and factors.vol_252 > 0:
            factors.smoothness = factors.ret_12m / factors.vol_252

    # Max drawdown over last 252 days
    win_252 = close.iloc[-252:] if len(close) >= 252 else close
    factors.mdd_252 = _max_drawdown(win_252)
    if (
        factors.ret_12m is not None
        and factors.mdd_252 is not None
        and factors.mdd_252 < 0
    ):
        factors.calmar = factors.ret_12m / abs(factors.mdd_252)

    # Linear regression R² on log-close, last 126 sessions
    log_close = np.log(close.iloc[-126:]) if len(close) >= 126 else None
    if log_close is not None:
        factors.r_sq_126 = _r_squared_linear(log_close)

    # SPX-relative metrics
    if spx_log_returns is not None and len(log_ret) > 0:
        joined = pd.concat([log_ret.rename("stk"), spx_log_returns.rename("spx")], axis=1).dropna()
        recent = joined.iloc[-252:] if len(joined) > 252 else joined
        if len(recent) >= 60:
            cov = float(recent["stk"].cov(recent["spx"]))
            var_spx = float(recent["spx"].var())
            factors.corr_spx = float(recent["stk"].corr(recent["spx"]))
            factors.beta_spx = (cov / var_spx) if var_spx > 0 else None

            down_days = recent[recent["spx"] <= -0.01]
            if len(down_days) >= 5:
                factors.down_capture = float(down_days["stk"].mean())

    # Above 200d SMA — confirms it's actually trending up
    if len(close) >= 200:
        sma_200 = float(close.iloc[-200:].mean())
        factors.above_200d = (last / sma_200 - 1.0) if sma_200 > 0 else None

    # Trend-health signals — for ride/eject decisions
    if len(close) >= 21:
        ema_21 = close.ewm(span=21, adjust=False).mean().iloc[-1]
        factors.above_21d_ema = bool(last > ema_21)
    if len(close) >= 50:
        sma_50 = float(close.iloc[-50:].mean())
        factors.above_50d_sma = bool(last > sma_50)

    # Distance from 52w high (negative number = below the high; 0 = at high)
    if len(close) >= 252:
        win = close.iloc[-252:]
    else:
        win = close
    high_52w = float(win.max())
    if high_52w > 0:
        factors.pct_from_52w_high = (last / high_52w) - 1.0
        # Days since 52w high: index distance from current bar to the bar
        # where the 52w high was set. Smaller = trend still printing fresh highs.
        try:
            idx_of_high = int(win.idxmax())
            factors.days_since_52w_high = max(0, len(close) - 1 - idx_of_high)
        except (ValueError, KeyError):
            factors.days_since_52w_high = None

    # Recent (60d) drawdown from trailing peak — captures live pullback depth
    if len(close) >= 60:
        recent = close.iloc[-60:]
        recent_peak = float(recent.max())
        if recent_peak > 0:
            factors.pct_from_recent_high_60d = (last / recent_peak) - 1.0
        c0_60 = float(close.iloc[-60])
        if c0_60 > 0:
            factors.recent_60d_ret = last / c0_60 - 1.0

    # Composite trend status — what the user reads to decide ride / eject.
    factors.trend_status = _classify_trend(factors)

    # ATR (20d) as % of current price — used by Path P pullback-continuation
    # entry rule. The same calc that atr_adj_gap uses internally; we now
    # also store it on the dataclass for downstream consumers.
    if len(close) >= 21:
        high_s = bars["high"].astype(float).reset_index(drop=True)
        low_s = bars["low"].astype(float).reset_index(drop=True)
        close_s = close.reset_index(drop=True)
        prev_close_s = close_s.shift(1)
        tr = pd.concat(
            [(high_s - low_s).abs(),
             (high_s - prev_close_s).abs(),
             (low_s - prev_close_s).abs()],
            axis=1,
        ).max(axis=1)
        atr_20 = float(tr.iloc[-20:].mean()) if len(tr) >= 20 else None
        if atr_20 is not None and last > 0:
            factors.atr_pct = atr_20 / last

    # Volatility regime: 20d vs 60d annualized stdev of log returns
    if len(log_ret) >= 60:
        v20 = float(log_ret.iloc[-20:].std() * np.sqrt(252))
        v60 = float(log_ret.iloc[-60:].std() * np.sqrt(252))
        factors.vol_20d = v20
        factors.vol_60d = v60
        if v60 > 0:
            factors.coil_ratio = v20 / v60

    # Liquidity
    dv = (close * volume).iloc[-20:]
    factors.dollar_vol_20d = float(dv.median()) if len(dv) else 0.0

    return factors


def _classify_trend(f: "StabilityFactors") -> str:
    """Categorize current trend health.

    INTACT   — above 21d EMA + 50d + 200d AND within 8% of 52w high
    PULLBACK — above 50d but below 21d EMA (healthy pullback in uptrend)
    WARNING  — below 50d but above 200d (medium-term break)
    BROKEN   — below 200d (trend over)
    UNKNOWN  — not enough data
    """
    if f.above_50d_sma is None or f.above_200d is None:
        return "UNKNOWN"
    if not f.above_200d_or_zero():
        return "BROKEN"
    if not f.above_50d_sma:
        return "WARNING"
    # above 50d and 200d
    if f.above_21d_ema is False:
        return "PULLBACK"
    # all three MAs aligned
    if f.pct_from_52w_high is not None and f.pct_from_52w_high >= -0.08:
        return "INTACT"
    return "PULLBACK"


def _zscore(s: pd.Series) -> pd.Series:
    s = s.copy()
    valid = s.dropna()
    if len(valid) < 2 or valid.std(ddof=0) == 0:
        return s.fillna(0.0) * 0.0
    return ((s - valid.mean()) / valid.std(ddof=0)).fillna(0.0)


def rank_universe(
    factors_by_ticker: dict[str, StabilityFactors],
    *,
    min_dollar_volume: float = 25_000_000,
    min_ret_12m: float = 0.0,
    max_ret_12m: float = 5.0,  # 500% — anything higher is almost always a spinoff/IPO bar artifact
) -> pd.DataFrame:
    """Rank survivors by composite stability score.

    Composite blends: smoothness (Sharpe-like), low max drawdown, high R²
    (linear up-trend), low SPX correlation, positive down-day capture.
    Higher = better.
    """
    rows = []
    for tkr, f in factors_by_ticker.items():
        rows.append({
            "ticker": tkr,
            "last_close": f.last_close,
            "ret_12m": f.ret_12m,
            "recent_60d_ret": f.recent_60d_ret,
            "vol_252": f.vol_252,
            "smoothness": f.smoothness,
            "mdd_252": f.mdd_252,
            "calmar": f.calmar,
            "r_sq_126": f.r_sq_126,
            "corr_spx": f.corr_spx,
            "beta_spx": f.beta_spx,
            "down_capture": f.down_capture,
            "above_200d": f.above_200d,
            "above_50d_sma": f.above_50d_sma,
            "above_21d_ema": f.above_21d_ema,
            "pct_from_52w_high": f.pct_from_52w_high,
            "pct_from_recent_high_60d": f.pct_from_recent_high_60d,
            "days_since_52w_high": f.days_since_52w_high,
            "trend_status": f.trend_status,
            "dollar_vol_20d": f.dollar_vol_20d,
        })
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("ticker")
    eligible = pd.Series(True, index=df.index)
    eligible &= df["dollar_vol_20d"].fillna(0) >= min_dollar_volume
    eligible &= df["ret_12m"].fillna(-1) >= min_ret_12m
    eligible &= df["ret_12m"].fillna(0) <= max_ret_12m
    # High-flyer filter: trend must be currently INTACT or PULLBACK (alive).
    # Throws out names already breaking down (WARNING) or fully busted (BROKEN).
    eligible &= df["trend_status"].isin(["INTACT", "PULLBACK"])
    df["eligible"] = eligible

    # Composite weights tuned for "smooth mega-gainer" — the WDC / NVDA /
    # PLTR archetype: a chart that's gone up *a lot* but in a clean line,
    # with shallow drawdowns. High beta is fine (these names AMPLIFY market
    # moves) — what we hate is choppiness and big drawdowns within the
    # uptrend. R² captures the "straight line" the user described literally.
    weights = {
        "calmar": 2.0,            # return / max-drawdown — return per unit of pain
        "r_sq_126": 2.0,          # straight-line trajectory (linear log-price fit)
        "smoothness": 1.5,        # Sharpe-like — return per unit of vol
        "ret_12m": 1.0,           # need real return to qualify as mega-gainer
        "above_200d": 0.5,        # still trending up now, not topped out
        # Deliberately NOT scoring corr_spx or down_capture — mega-gainers
        # ARE correlated, that's how compounding works in a bull tape.
    }

    sub = df.loc[eligible]
    if sub.empty:
        df["composite"] = np.nan
        return df.sort_values(["eligible", "ret_12m"], ascending=[False, False])

    parts = []
    for col, w in weights.items():
        z = _zscore(sub[col])
        parts.append(w * z)
    composite = pd.concat(parts, axis=1).sum(axis=1)
    df["composite"] = np.nan
    df.loc[composite.index, "composite"] = composite
    return df.sort_values("composite", ascending=False)

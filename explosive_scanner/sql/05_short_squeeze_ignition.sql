-- Short-Squeeze Ignition companion SQL.
--
-- Assumptions:
--   * Daily bars live in `market.stock_daily_bars`
--   * FINRA short interest files are ingested into `market.short_interest`
--   * Trading calendar lives in `market.trading_calendar`
--   * Optional Benzinga news history lives in `market.benzinga_news`
--
-- This keeps the user's point-in-time rules intact:
--   * join short interest by availability date, not settlement date
--   * use days_to_cover rather than raw short-interest percentile
--   * keep float out of the historical backtest unless you have PIT float

CREATE OR REPLACE TEMP VIEW squeeze_bar_features AS
WITH base AS (
    SELECT
        ticker,
        dt,
        open,
        high,
        low,
        close,
        volume,
        close * volume AS dollar_volume,
        LAG(close) OVER (
            PARTITION BY ticker
            ORDER BY dt
        ) AS prev_close,
        AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY dt
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_vol_20,
        AVG(close * volume) OVER (
            PARTITION BY ticker
            ORDER BY dt
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_dollar_vol_20
    FROM market.stock_daily_bars
),
features AS (
    SELECT
        *,
        CASE
            WHEN prev_close > 0 THEN close / prev_close - 1
        END AS ret_1d,
        CASE
            WHEN avg_vol_20 > 0 THEN volume / avg_vol_20
        END AS rel_vol_20,
        CASE
            WHEN high > low THEN (close - low) / (high - low)
        END AS close_location
    FROM base
)
SELECT *
FROM features
WHERE close >= 2
  AND avg_dollar_vol_20 >= 5000000;


CREATE OR REPLACE TEMP VIEW short_interest_available AS
WITH settle_cal AS (
    SELECT
        si.ticker,
        si.settlement_date,
        si.current_short_interest,
        si.previous_short_interest,
        si.average_daily_volume,
        si.days_to_cover,
        si.short_interest_change_pct,
        c.trading_day_number AS settlement_tdn
    FROM market.short_interest si
    JOIN market.trading_calendar c
      ON si.settlement_date = c.dt
),
available AS (
    SELECT
        s.*,
        c2.dt AS si_available_date
    FROM settle_cal s
    JOIN market.trading_calendar c2
      ON c2.trading_day_number = s.settlement_tdn + 7
)
SELECT *
FROM available;


CREATE OR REPLACE TEMP VIEW squeeze_with_short_interest AS
WITH joined AS (
    SELECT
        b.*,
        si.settlement_date,
        si.si_available_date,
        si.current_short_interest,
        si.previous_short_interest,
        si.average_daily_volume,
        si.days_to_cover,
        si.short_interest_change_pct,
        ROW_NUMBER() OVER (
            PARTITION BY b.ticker, b.dt
            ORDER BY si.si_available_date DESC
        ) AS rn
    FROM squeeze_bar_features b
    LEFT JOIN short_interest_available si
      ON b.ticker = si.ticker
     AND si.si_available_date <= b.dt
)
SELECT *
FROM joined
WHERE rn = 1;


CREATE OR REPLACE TEMP VIEW squeeze_ranked AS
WITH ranked AS (
    SELECT
        *,
        PERCENT_RANK() OVER (
            PARTITION BY dt
            ORDER BY days_to_cover
        ) AS days_to_cover_pctile,
        PERCENT_RANK() OVER (
            PARTITION BY dt
            ORDER BY rel_vol_20
        ) AS rel_vol_pctile,
        PERCENT_RANK() OVER (
            PARTITION BY dt
            ORDER BY ret_1d
        ) AS ret_1d_pctile
    FROM squeeze_with_short_interest
    WHERE days_to_cover IS NOT NULL
)
SELECT
    *,
    0.35 * days_to_cover_pctile
  + 0.25 * rel_vol_pctile
  + 0.20 * ret_1d_pctile
  + 0.20 * close_location AS squeeze_score
FROM ranked;


CREATE OR REPLACE TEMP VIEW short_squeeze_core_ignition AS
SELECT *
FROM squeeze_ranked
WHERE days_to_cover >= 3
  AND days_to_cover_pctile >= 0.80
  AND ret_1d >= 0.12
  AND rel_vol_20 >= 5
  AND close_location >= 0.75
  AND avg_dollar_vol_20 >= 5000000
  AND close >= 2;


CREATE OR REPLACE TEMP VIEW short_squeeze_nuclear_ignition AS
SELECT *
FROM squeeze_ranked
WHERE days_to_cover >= 5
  AND days_to_cover_pctile >= 0.90
  AND ret_1d >= 0.15
  AND rel_vol_20 >= 7
  AND close_location >= 0.85
  AND avg_dollar_vol_20 >= 10000000
  AND close >= 3;


CREATE OR REPLACE TEMP VIEW short_squeeze_rising_short_interest_ignition AS
SELECT *
FROM squeeze_ranked
WHERE short_interest_change_pct >= 0.10
  AND days_to_cover >= 3
  AND ret_1d >= 0.10
  AND rel_vol_20 >= 4
  AND close_location >= 0.75;


CREATE OR REPLACE TEMP VIEW short_squeeze_catalyst_confirmed_ignition AS
WITH catalyst_hits AS (
    SELECT DISTINCT
        s.ticker,
        s.dt
    FROM short_squeeze_core_ignition s
    JOIN market.trading_calendar c
      ON c.dt = s.dt
    JOIN market.trading_calendar c_start
      ON c_start.trading_day_number = c.trading_day_number - 2
    JOIN market.benzinga_news n
      ON n.ticker = s.ticker
     AND n.published_at >= c_start.dt
     AND n.published_at < DATEADD(day, 1, s.dt)
    WHERE LOWER(
            CONCAT_WS(
                ' ',
                COALESCE(n.title, ''),
                COALESCE(n.body, ''),
                COALESCE(n.channel, '')
            )
        ) RLIKE 'earnings|guidance|raises? outlook|contract|partnership|strategic review|activist|buyback|acquisition|fda approval|settlement|debt refinancing'
)
SELECT s.*
FROM short_squeeze_core_ignition s
JOIN catalyst_hits h
  ON s.ticker = h.ticker
 AND s.dt = h.dt;


CREATE OR REPLACE TEMP VIEW short_squeeze_second_day_confirmation AS
WITH day1 AS (
    SELECT
        ticker,
        dt AS signal_day_1,
        close AS close_day_1,
        volume AS volume_day_1
    FROM squeeze_ranked
    WHERE days_to_cover >= 3
      AND ret_1d >= 0.12
      AND rel_vol_20 >= 5
      AND close_location >= 0.75
),
day1_next_day AS (
    SELECT
        d1.ticker,
        d1.signal_day_1,
        d1.close_day_1,
        d1.volume_day_1,
        c2.dt AS signal_day_2
    FROM day1 d1
    JOIN market.trading_calendar c1
      ON c1.dt = d1.signal_day_1
    JOIN market.trading_calendar c2
      ON c2.trading_day_number = c1.trading_day_number + 1
),
day2 AS (
    SELECT
        s.*,
        d1.signal_day_1,
        d1.close_day_1,
        d1.volume_day_1
    FROM squeeze_ranked s
    JOIN day1_next_day d1
      ON s.ticker = d1.ticker
     AND s.dt = d1.signal_day_2
)
SELECT *
FROM day2
WHERE close > close_day_1
  AND volume >= 0.75 * volume_day_1
  AND close_location >= 0.60;

--query test
select * from securities order by symbol 
select * from daily_ohlc where symbol = 'SSI' order by trading_date DESC ;
select * from daily_stock_prices where symbol = 'SSI' order by trading_date DESC ;
select * from daily_stock_prices where symbol = 'SVD' and trading_date = '2025-06-12';

select count(*)from securities where length(symbol) = 3 and market = 'HOSE'; 

SELECT 
    COUNT(*) AS total_rows,
    MIN(trading_date) AS start_date,
    MAX(trading_date) AS end_date
FROM daily_ohlc
WHERE symbol = 'TNI';

SELECT 
    symbol, 
    COUNT(*) as total_records, 
    MIN(trading_date) as start_date, 
    MAX(trading_date) as end_date
FROM daily_stock_prices
WHERE symbol = 'SSI'
GROUP BY symbol 
ORDER BY total_records ASC;

SELECT 
    symbol, 
    COUNT(*) as total_records, 
    MIN(trading_date) as start_date, 
    MAX(trading_date) as end_date
FROM daily_stock_prices
GROUP BY symbol 
ORDER BY total_records ASC;

SELECT 
    symbol, 
    MAX(trading_date) as latest_day,
    COUNT(*) as total_rows
FROM daily_stock_prices
GROUP BY symbol
HAVING MAX(trading_date) < '2026-02-25' 
ORDER BY latest_day ASC;

SELECT 
    symbol, 
    trading_date, 
    LEAD(trading_date) OVER (ORDER BY trading_date) as next_date,
    LEAD(trading_date) OVER (ORDER BY trading_date) - trading_date as gap
FROM daily_stock_prices
WHERE symbol = 'DBD'
ORDER BY trading_date;

WITH price_gaps AS (
    SELECT 
        symbol,
        trading_date,
        LEAD(trading_date) OVER (
            PARTITION BY symbol 
            ORDER BY trading_date
        ) AS next_date,
        LEAD(trading_date) OVER (
            PARTITION BY symbol 
            ORDER BY trading_date
        ) - trading_date AS gap
    FROM daily_stock_prices
    WHERE symbol = 'SSI'
)
SELECT *
FROM price_gaps
WHERE gap > 3
ORDER BY trading_date;

SELECT s.trading_date
FROM (SELECT trading_date FROM daily_stock_prices WHERE symbol = 'SSI') s
LEFT JOIN (SELECT trading_date FROM daily_stock_prices WHERE symbol = 'DPR') m
ON s.trading_date = m.trading_date
WHERE m.trading_date IS NULL;

WITH date_series AS (
    SELECT trading_date,
           LEAD(trading_date) OVER (ORDER BY trading_date) as next_date
    FROM daily_stock_prices
    WHERE symbol = 'NVL'
),
gaps AS (
    SELECT trading_date, next_date
    FROM date_series
    WHERE next_date - trading_date > 1
      AND next_date IS NOT NULL
)
SELECT g.trading_date, g.next_date,
       COUNT(tc.trading_date) as missing_trading_days
FROM gaps g
JOIN trading_calendar tc 
    ON tc.trading_date > g.trading_date 
    AND tc.trading_date < g.next_date
    AND tc.is_trading_day = TRUE
GROUP BY g.trading_date, g.next_date
HAVING COUNT(tc.trading_date) > 0
ORDER BY g.trading_date;

---------------------------------------------------
SELECT symbol, stock_name, market
FROM securities
WHERE symbol IN (
    SELECT symbol FROM securities
    EXCEPT
    (
        SELECT symbol FROM daily_stock_prices
    )
);
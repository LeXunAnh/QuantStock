-- 1. Bảng danh mục (Cần chạy trước để làm bảng gốc cho khóa ngoại)
CREATE TABLE IF NOT EXISTS securities (
    symbol VARCHAR(20) PRIMARY KEY,
    market VARCHAR(20),
    stock_name VARCHAR(255),
    stock_en_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE daily_ohlc (
    symbol VARCHAR(20) NOT NULL REFERENCES securities(symbol),
    trading_date DATE NOT NULL,
    open_price NUMERIC(18, 2),
    highest_price NUMERIC(18, 2),
    lowest_price NUMERIC(18, 2),
    close_price NUMERIC(18, 2),
    volume BIGINT,
    total_value NUMERIC(25, 2), -- Giá trị giao dịch thường rất lớn
    PRIMARY KEY (symbol, trading_date)
);

CREATE TABLE daily_stock_prices (
    symbol VARCHAR(20) NOT NULL
	REFERENCES securities(symbol)
	ON UPDATE CASCADE
	ON DELETE RESTRICT,
	
	-- Giá và Biến động
    trading_date DATE NOT NULL,
    price_change NUMERIC(18, 2),
    per_price_change NUMERIC(10, 2),
    ceiling_price NUMERIC(18, 2),
    floor_price NUMERIC(18, 2),
    ref_price NUMERIC(18, 2),
    open_price NUMERIC(18, 2),
    highest_price NUMERIC(18, 2),
    lowest_price NUMERIC(18, 2),
    close_price NUMERIC(18, 2),
    average_price NUMERIC(18, 2),
    close_price_adjusted NUMERIC(18, 5),

	-- Khối lượng và Giá trị
    total_match_vol BIGINT,
    total_match_val BIGINT,
    total_deal_vol BIGINT,
    total_deal_val BIGINT,
	
	-- Khối ngoại
    foreign_buy_vol_total BIGINT,
    foreign_sell_vol_total BIGINT,
    foreign_buy_val_total BIGINT,
    foreign_sell_val_total BIGINT,
    foreign_current_room BIGINT,
    net_buy_sell_vol BIGINT,
    net_buy_sell_val BIGINT,
	
	-- Thống kê khác
    total_traded_vol BIGINT,
    total_traded_value BIGINT,
    total_buy_trade INT,
    total_buy_trade_vol BIGINT,
    total_sell_trade INT,
    total_sell_trade_vol BIGINT,

	-- Metadata
    time_str VARCHAR(20),
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trading_date)
);

CREATE TABLE trading_calendar (
    trading_date DATE PRIMARY KEY,
    is_trading_day BOOLEAN NOT NULL DEFAULT TRUE,
    note VARCHAR(100) );

INSERT INTO trading_calendar (trading_date, is_trading_day)
SELECT DISTINCT trading_date, TRUE
FROM daily_ohlc
WHERE symbol IN ('VCB', 'VNM', 'HPG', 'VIC', 'VHM')  -- Blue-chip, ít bị suspend nhất
  AND trading_date >= '2021-01-01'
ON CONFLICT (trading_date) DO NOTHING;


-- Index để truy vấn nhanh
CREATE INDEX idx_daily_date ON daily_stock_prices (trading_date);
CREATE INDEX idx_daily_symbol_date ON daily_stock_prices (symbol, trading_date DESC);
CREATE INDEX idx_ohlc_symbol_date ON daily_ohlc (symbol, trading_date DESC);
CREATE INDEX idx_trading_calendar_date ON trading_calendar (trading_date);


--query test
select * from securities order by symbol 
select * from daily_ohlc where symbol = 'SSI' order by trading_date DESC ;
select * from daily_stock_prices where symbol = 'EIB' order by trading_date DESC ;
select * from daily_stock_prices where symbol = 'SVD' and trading_date = '2025-06-12';

select count(*)from securities where length(symbol) = 3 and market = 'HOSE'; 

SELECT 
    COUNT(*) AS total_rows,
    MIN(trading_date) AS start_date,
    MAX(trading_date) AS end_date
FROM daily_ohlc
WHERE symbol = 'SSI';

SELECT 
    symbol, 
    COUNT(*) as total_records, 
    MIN(trading_date) as start_date, 
    MAX(trading_date) as end_date
FROM daily_stock_prices
WHERE symbol = 'ANV'
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



DROP TABLE IF EXISTS securities;
DROP TABLE IF EXISTS daily_ohlc;
DROP TABLE IF EXISTS daily_stock_prices;
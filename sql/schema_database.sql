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
CREATE INDEX idx_securities_market ON securities(market);






DROP TABLE IF EXISTS securities;
DROP TABLE IF EXISTS daily_ohlc;
DROP TABLE IF EXISTS daily_stock_prices;
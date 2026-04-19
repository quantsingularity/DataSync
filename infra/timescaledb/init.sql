-- DataSync - TimescaleDB Schema

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Normalized tick data (equities, crypto, options)
CREATE TABLE IF NOT EXISTS ticks (
    time        TIMESTAMPTZ     NOT NULL,
    symbol      TEXT            NOT NULL,
    asset_class TEXT            NOT NULL,  -- equity | crypto | option
    source      TEXT            NOT NULL,  -- alpaca | polygon | mock
    price       NUMERIC(18, 8)  NOT NULL,
    size        NUMERIC(18, 8),
    bid         NUMERIC(18, 8),
    ask         NUMERIC(18, 8),
    bid_size    NUMERIC(18, 8),
    ask_size    NUMERIC(18, 8),
    conditions  TEXT[],
    exchange    TEXT,
    tape        TEXT,
    extra       JSONB
);

SELECT create_hypertable('ticks', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_time ON ticks (symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_ticks_asset_class  ON ticks (asset_class, time DESC);

-- OHLCV bars (aggregated candles)
CREATE TABLE IF NOT EXISTS bars (
    time        TIMESTAMPTZ     NOT NULL,
    symbol      TEXT            NOT NULL,
    asset_class TEXT            NOT NULL,
    timeframe   TEXT            NOT NULL,  -- 1m | 5m | 15m | 1h | 1d
    source      TEXT            NOT NULL,
    open        NUMERIC(18, 8)  NOT NULL,
    high        NUMERIC(18, 8)  NOT NULL,
    low         NUMERIC(18, 8)  NOT NULL,
    close       NUMERIC(18, 8)  NOT NULL,
    volume      NUMERIC(24, 4),
    trade_count INT,
    vwap        NUMERIC(18, 8)
);

SELECT create_hypertable('bars', 'time', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS idx_bars_symbol_tf_time
    ON bars (symbol, timeframe, time DESC);

-- Alternative data table
CREATE TABLE IF NOT EXISTS alt_data (
    time        TIMESTAMPTZ     NOT NULL,
    symbol      TEXT,
    data_type   TEXT            NOT NULL,  -- news_sentiment | social_volume | onchain
    source      TEXT            NOT NULL,
    score       NUMERIC(8, 4),
    volume      NUMERIC(24, 4),
    payload     JSONB           NOT NULL
);

SELECT create_hypertable('alt_data', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_alt_symbol_time ON alt_data (symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_alt_type_time   ON alt_data (data_type, time DESC);

-- Symbol subscriptions
CREATE TABLE IF NOT EXISTS subscriptions (
    id            SERIAL PRIMARY KEY,
    service_name  TEXT            NOT NULL,
    symbol        TEXT            NOT NULL,
    asset_class   TEXT            NOT NULL,
    is_active     BOOLEAN         NOT NULL DEFAULT TRUE,
    subscribed_at TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (service_name, symbol)
);

-- Continuous aggregate: 1-minute OHLCV from ticks
CREATE MATERIALIZED VIEW IF NOT EXISTS ticks_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    symbol,
    asset_class,
    first(price, time)            AS open,
    max(price)                    AS high,
    min(price)                    AS low,
    last(price, time)             AS close,
    sum(size)                     AS volume,
    count(*)                      AS trade_count
FROM ticks
GROUP BY bucket, symbol, asset_class
WITH NO DATA;

-- Seed default subscriptions
INSERT INTO subscriptions (service_name, symbol, asset_class) VALUES
    ('datasync', 'AAPL',    'equity'),
    ('datasync', 'MSFT',    'equity'),
    ('datasync', 'GOOGL',   'equity'),
    ('datasync', 'AMZN',    'equity'),
    ('datasync', 'TSLA',    'equity'),
    ('datasync', 'SPY',     'equity'),
    ('datasync', 'BTC/USD', 'crypto'),
    ('datasync', 'ETH/USD', 'crypto'),
    ('datasync', 'SOL/USD', 'crypto')
ON CONFLICT DO NOTHING;

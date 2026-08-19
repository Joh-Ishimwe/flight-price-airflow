-- Schema only, runs once on first container start. No data loaded here.
-- Analytics DB: published, trustworthy tables. Everything here came from a
-- validated MySQL batch - nothing raw or unchecked lands here directly.

-- One row per load from MySQL staging into here. Mirrors ingestion_runs.
CREATE TABLE IF NOT EXISTS load_runs (
    batch_id        VARCHAR(64) PRIMARY KEY,
    rows_loaded     INT,
    status          VARCHAR(20) NOT NULL
                    CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    error_message   TEXT,
    started_at      TIMESTAMP   NOT NULL,
    finished_at     TIMESTAMP
);

-- Curated fact table: one row per valid booking, Total Fare recomputed.
-- Append-only, same as staging - each load adds its own batch.
CREATE TABLE IF NOT EXISTS fact_flight_prices (
    id                      BIGINT       NOT NULL,
    airline                 VARCHAR(100),
    source_code             VARCHAR(10),
    source_name             VARCHAR(255),
    destination_code        VARCHAR(10),
    destination_name        VARCHAR(255),
    departure_datetime      TIMESTAMP,
    arrival_datetime        TIMESTAMP,
    duration_hrs            NUMERIC(10, 6),
    stopovers               VARCHAR(50),
    aircraft_type           VARCHAR(100),
    travel_class            VARCHAR(50),
    booking_source          VARCHAR(100),
    base_fare_bdt           NUMERIC(14, 4),
    tax_surcharge_bdt       NUMERIC(14, 4),
    total_fare_bdt          NUMERIC(14, 4),
    seasonality             VARCHAR(50),
    days_before_departure   INT,
    batch_id                VARCHAR(64)  NOT NULL,
    loaded_at               TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, id),
    FOREIGN KEY (batch_id) REFERENCES load_runs(batch_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_airline ON fact_flight_prices (airline);
CREATE INDEX IF NOT EXISTS idx_fact_route ON fact_flight_prices (source_code, destination_code);

-- KPI tables: one snapshot per batch, not overwritten - lets KPIs be
-- compared across loads instead of only showing "right now".
-- Booking Count by Airline is folded into avg_fare_by_airline
-- (booking_count column) since it's the same group-by, not a separate cut.

CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    batch_id        VARCHAR(64)   NOT NULL,
    airline         VARCHAR(100)  NOT NULL,
    avg_fare        NUMERIC(14, 4),
    booking_count   INT,
    computed_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, airline),
    FOREIGN KEY (batch_id) REFERENCES load_runs(batch_id)
);

CREATE TABLE IF NOT EXISTS kpi_seasonal_fare (
    batch_id        VARCHAR(64)   NOT NULL,
    seasonality     VARCHAR(50)   NOT NULL,
    avg_fare        NUMERIC(14, 4),
    booking_count   INT,
    is_peak         BOOLEAN,
    computed_at     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, seasonality),
    FOREIGN KEY (batch_id) REFERENCES load_runs(batch_id)
);

CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    batch_id            VARCHAR(64)  NOT NULL,
    rank                INT          NOT NULL,
    source_code         VARCHAR(10),
    destination_code    VARCHAR(10),
    booking_count       INT,
    computed_at         TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, rank),
    FOREIGN KEY (batch_id) REFERENCES load_runs(batch_id)
);

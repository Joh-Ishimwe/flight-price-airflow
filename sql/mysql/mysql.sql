-- Schema only, runs once on first container start. No data loaded here.

CREATE DATABASE IF NOT EXISTS flight_staging
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE flight_staging;

-- One row per pipeline run: did it work, how many rows moved.
CREATE TABLE IF NOT EXISTS ingestion_runs (
    batch_id        VARCHAR(64) PRIMARY KEY,
    source_file     VARCHAR(255)  NOT NULL,
    rows_in_source  INT,
    rows_loaded     INT,
    status          VARCHAR(20)   NOT NULL
                    CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    error_message   TEXT,
    started_at      DATETIME      NOT NULL,
    finished_at     DATETIME
) ENGINE=InnoDB;

-- Raw landing table, one column per CSV column. No validation here -
-- staging accepts whatever the source gave us. Append-only: each run
-- adds its own batch instead of overwriting the last one.
CREATE TABLE IF NOT EXISTS stg_flight_prices (
    id                      BIGINT AUTO_INCREMENT,
    airline                 VARCHAR(100),
    source_code             VARCHAR(10),
    source_name             VARCHAR(255),
    destination_code        VARCHAR(10),
    destination_name        VARCHAR(255),
    departure_datetime      DATETIME,
    arrival_datetime        DATETIME,
    duration_hrs            DECIMAL(10, 6),
    stopovers               VARCHAR(50),
    aircraft_type           VARCHAR(100),
    travel_class            VARCHAR(50),
    booking_source          VARCHAR(100),
    base_fare_bdt           DECIMAL(14, 4),
    tax_surcharge_bdt       DECIMAL(14, 4),
    total_fare_bdt          DECIMAL(14, 4),
    seasonality             VARCHAR(50),
    days_before_departure   INT,

    -- Lineage: which run produced this row.
    source_file             VARCHAR(255),
    ingested_at             DATETIME     DEFAULT CURRENT_TIMESTAMP,
    batch_id                VARCHAR(64)  NOT NULL,

    PRIMARY KEY (batch_id, id),
    UNIQUE INDEX idx_id (id),
    FOREIGN KEY (batch_id) REFERENCES ingestion_runs(batch_id),
    INDEX idx_airline (airline),
    INDEX idx_route (source_code, destination_code)
) ENGINE=InnoDB;

-- Validation outcome per staged row. Kept separate from stg_flight_prices
-- so the landing table stays untouched - flag rows, don't delete them.
CREATE TABLE IF NOT EXISTS stg_validation_results (
    id              BIGINT        NOT NULL,
    batch_id        VARCHAR(64)   NOT NULL,
    is_valid        BOOLEAN       NOT NULL,
    reasons         VARCHAR(500),
    validated_at    DATETIME      DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (batch_id, id),
    FOREIGN KEY (id) REFERENCES stg_flight_prices(id),
    FOREIGN KEY (batch_id) REFERENCES ingestion_runs(batch_id),
    INDEX idx_batch_valid (batch_id, is_valid)
) ENGINE=InnoDB;

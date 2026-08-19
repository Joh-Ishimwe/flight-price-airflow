CREATE DATABASE IF NOT EXISTS flight_staging
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE flight_staging;

CREATE TABLE IF NOT EXISTS stg_flight_prices (
    id                      INT AUTO_INCREMENT PRIMARY KEY,
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
    ingested_at             DATETIME     DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_airline (airline),
    INDEX idx_route (source_code, destination_code)
) ENGINE=InnoDB;

DESCRIBE stg_flight_prices;
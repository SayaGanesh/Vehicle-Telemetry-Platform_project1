-- ============================================================
-- Vehicle Telemetry Analytics – Synapse SQL Views & Queries
-- Target: Azure Synapse Analytics (Serverless SQL Pool)
-- Gold Layer Delta tables exposed via external tables
-- ============================================================

-- ------------------------------------------------------------
-- 1. Create External Data Source pointing to Gold ADLS path
-- ------------------------------------------------------------
CREATE EXTERNAL DATA SOURCE gold_vehicle_telemetry
WITH (
    LOCATION = 'abfss://gold@yourstorageaccount.dfs.core.windows.net/'
);

-- ------------------------------------------------------------
-- 2. View: Fleet Daily Summary
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vw_fleet_daily_summary AS
SELECT
    event_date,
    COUNT(DISTINCT vehicle_id)          AS active_vehicles,
    ROUND(AVG(avg_speed_kmh), 2)        AS fleet_avg_speed_kmh,
    ROUND(AVG(health_score), 2)         AS fleet_avg_health_score,
    SUM(overspeed_events)               AS total_overspeed_events,
    SUM(overheat_events)                AS total_overheat_events,
    SUM(harsh_brake_events)             AS total_harsh_brake_events
FROM
    OPENROWSET(
        BULK 'vehicle_daily_metrics/',
        DATA_SOURCE = 'gold_vehicle_telemetry',
        FORMAT = 'DELTA'
    ) AS r
GROUP BY event_date
ORDER BY event_date DESC;

-- ------------------------------------------------------------
-- 3. View: Top 10 Riskiest Vehicles (last 30 days)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vw_high_risk_vehicles AS
SELECT TOP 10
    vehicle_id,
    SUM(overspeed_events)    AS total_overspeed,
    SUM(overheat_events)     AS total_overheat,
    SUM(harsh_brake_events)  AS total_harsh_brake,
    ROUND(AVG(health_score), 2) AS avg_health_score
FROM
    OPENROWSET(
        BULK 'vehicle_daily_metrics/',
        DATA_SOURCE = 'gold_vehicle_telemetry',
        FORMAT = 'DELTA'
    ) AS r
WHERE event_date >= DATEADD(DAY, -30, GETDATE())
GROUP BY vehicle_id
ORDER BY avg_health_score ASC;

-- ------------------------------------------------------------
-- 4. Query: Daily Anomaly Trend (for Power BI)
-- ------------------------------------------------------------
SELECT
    event_date,
    SUM(overspeed_events)   AS overspeed,
    SUM(overheat_events)    AS overheat,
    SUM(harsh_brake_events) AS harsh_brake,
    SUM(low_fuel_events)    AS low_fuel
FROM
    OPENROWSET(
        BULK 'vehicle_daily_metrics/',
        DATA_SOURCE = 'gold_vehicle_telemetry',
        FORMAT = 'DELTA'
    ) AS r
GROUP BY event_date
ORDER BY event_date;

-- ------------------------------------------------------------
-- 5. Query: Vehicle Health Score Ranking
-- ------------------------------------------------------------
SELECT
    vehicle_id,
    event_date,
    health_score,
    RANK() OVER (PARTITION BY event_date ORDER BY health_score DESC) AS daily_rank
FROM
    OPENROWSET(
        BULK 'vehicle_daily_metrics/',
        DATA_SOURCE = 'gold_vehicle_telemetry',
        FORMAT = 'DELTA'
    ) AS r
WHERE health_score IS NOT NULL
ORDER BY event_date DESC, daily_rank;

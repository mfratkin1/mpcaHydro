CREATE OR REPLACE VIEW derived.baseflow AS

WITH unit_converted AS (
    SELECT *,
        CASE
            WHEN LOWER(ts_unitsymbol) = 'ft³/s'  THEN 'cfs'
            ELSE ts_unitsymbol
        END AS unit
    FROM staging.wiski
),

normalized AS (
    SELECT
        station_no AS station_id,
        "Timestamp" AS datetime,
        "Value" AS value,
        unit,
        "Quality Code" AS quality_code,
        'wiski' AS station_origin,
        'QB' AS constituent
    FROM unit_converted u
),

quality_filtered AS (
    SELECT 
        n.*
    FROM normalized n
    INNER JOIN mappings.wiski_quality_codes wqc 
        ON n.quality_code = wqc.quality_code
    WHERE wqc.active = 1
),


--year_filtered AS (
--    SELECT *
--    FROM quality_filtered
--    WHERE year(datetime) >= getvariable('min_year')
--),

hourly_averaged AS (
    SELECT
        station_id, constituent,
        DATE_TRUNC('hour', datetime + INTERVAL '30 minute') AS datetime,
        AVG(value) AS value,
        unit, station_origin
    FROM quality_filtered
    GROUP BY station_id, constituent, datetime, unit, station_origin
)

SELECT * FROM hourly_averaged;
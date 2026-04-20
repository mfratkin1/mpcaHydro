CREATE OR REPLACE VIEW analytics.equis AS

WITH mapped AS (
    -- Step 1: map_constituents
    SELECT
        e.*,
        m.constituent
    FROM staging.equis e
    JOIN mappings.equis_casrn m ON e.CAS_RN = m.cas_rn
),

timezone_normalized AS (
    -- Step 2: normalize_timezone to UTC-6
    SELECT *,
        CASE
            WHEN SAMPLE_DATE_TIMEZONE = 'CST' THEN SAMPLE_DATE_TIME
            WHEN SAMPLE_DATE_TIMEZONE = 'CDT' THEN SAMPLE_DATE_TIME - INTERVAL '1 hour'
            ELSE SAMPLE_DATE_TIME
        END AS datetime
    FROM mapped
),

unit_converted AS (
    -- Step 3: convert_units
    SELECT *,
        CASE
            WHEN LOWER(RESULT_UNIT) = 'ug/l'           THEN RESULT_NUMERIC / 1000
            WHEN LOWER(RESULT_UNIT) = 'mg/g'            THEN RESULT_NUMERIC * 1000
            WHEN LOWER(RESULT_UNIT) IN ('deg c', 'degc') THEN (RESULT_NUMERIC * 9/5) + 32
            ELSE RESULT_NUMERIC
        END AS value,
        CASE
            WHEN LOWER(RESULT_UNIT) = 'ug/l'           THEN 'mg/L'
            WHEN LOWER(RESULT_UNIT) = 'mg/g'            THEN 'mg/L'
            WHEN LOWER(RESULT_UNIT) IN ('deg c', 'degc') THEN 'degF'
            ELSE RESULT_UNIT
        END AS unit
    FROM timezone_normalized
),

columns_normalized AS (
    -- Step 4: normalize_columns
    SELECT
        SYS_LOC_CODE AS station_id,
        constituent,
        datetime,
        value,
        unit,
        'equis' AS station_origin
    FROM unit_converted
),

nondetects_replaced AS (
    -- Step 5: replace_nondetects
    SELECT
        station_id, constituent, datetime,
        COALESCE(value, 0) AS value,
        unit, station_origin
    FROM columns_normalized
),
    
-- year_filtered AS (
--     -- Step 6: filter_years
--     SELECT * FROM nondetects_replaced
--     WHERE year(datetime) >= getvariable('min_year')
-- ),

hourly_averaged AS (
    -- Step 7: average_results
    SELECT
        station_id, constituent,
        DATE_TRUNC('hour', datetime + INTERVAL '30 minute') AS datetime,
        AVG(value) AS value,
        unit, station_origin
    FROM nondetects_replaced
    GROUP BY station_id, constituent, datetime, unit, station_origin
)              

SELECT * FROM hourly_averaged;
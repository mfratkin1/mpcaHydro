

-- staging.wiski schema copied for baseflow processing
CREATE OR REPLACE VIEW derived.baseflow AS 
SELECT 
    CAST(NULL AS DATETIME) AS "Timestamp",
    CAST(NULL AS DOUBLE) AS "Value",
    CAST(NULL AS BIGINT) AS "Quality Code",
    CAST(NULL AS VARCHAR) AS "Quality Code Name",
    CAST(NULL AS VARCHAR) AS ts_unitsymbol,
    CAST(NULL AS VARCHAR) AS ts_id,
    CAST(NULL AS VARCHAR) AS station_no,
    CAST(NULL AS VARCHAR) AS station_name,
    CAST(NULL AS VARCHAR) AS station_latitude,
    CAST(NULL AS VARCHAR) AS station_longitude,
WHERE FALSE;

-- CREATE OR REPLACE TABLE derived.baseflow (
--     datetime TIMESTAMP,
--     value DOUBLE,
--     station_id VARCHAR,
--     station_origin VARCHAR,
--     constituent VARCHAR,
--     unit VARCHAR
-- );
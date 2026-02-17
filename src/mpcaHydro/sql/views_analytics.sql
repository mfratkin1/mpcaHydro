-- views_analytics.sql
-- Views for the analytics schema

-- View: wiski_normalized
-- Normalized WISKI data with unit conversions and column renames
CREATE OR REPLACE VIEW analytics.wiski_normalized AS
SELECT 
    -- Convert °C to °F and keep other values unchanged
    CASE 
        WHEN LOWER(ts_unitsymbol) = '°c' THEN (value * 9.0 / 5.0) + 32
        WHEN ts_unitsymbol = 'kg' THEN value * 2.20462
        ELSE value
    END AS value,

    -- Normalize units
    CASE 
        WHEN LOWER(ts_unitsymbol) = '°c' THEN 'degf'
        WHEN ts_unitsymbol = 'kg' THEN 'lb'
        WHEN ts_unitsymbol = 'ft³/s' THEN 'cfs'
        ELSE ts_unitsymbol
    END AS unit,

    -- Normalize column names
    station_no AS station_id,
    Timestamp AS datetime,
    "Quality Code" AS quality_code,
    "Quality Code Name" AS quality_code_name,
    parametertype_id,
    constituent
FROM staging.wiski;

-- View: observations
-- Combined observations from equis and wiski processed tables
CREATE OR REPLACE VIEW analytics.observations AS
SELECT datetime, value, station_id, station_origin, constituent, unit
FROM analytics.equis
UNION ALL
SELECT datetime, value, station_id, station_origin, constituent, unit
FROM analytics.wiski;

-- View: outlet_observations
-- Links observations to model reaches via outlets
CREATE OR REPLACE VIEW analytics.outlet_observations AS 
SELECT
    o.datetime,
    os.outlet_id,
    o.constituent,
    AVG(o.value) AS value,
    COUNT(o.value) AS count
FROM
    analytics.observations AS o
INNER JOIN
    outlets.outlet_stations AS os 
    ON o.station_id = os.station_id AND o.station_origin = os.station_origin
WHERE os.outlet_id IS NOT NULL
GROUP BY
    os.outlet_id,
    o.constituent,
    o.datetime;

-- View: outlet_observations_with_flow
-- Outlet observations joined with flow and baseflow data
CREATE OR REPLACE VIEW analytics.outlet_observations_with_flow AS
WITH 
    baseflow_data AS (
        SELECT
            outlet_id,
            datetime,
            "value" AS baseflow_value
        FROM
            analytics.outlet_observations
        WHERE
            constituent = 'QB'
    ),

    flow_data AS (
        SELECT
            outlet_id,
            datetime,
            "value" AS flow_value
        FROM
            analytics.outlet_observations
        WHERE
            constituent = 'Q'
    ),

    constituent_data AS (
        SELECT
            outlet_id,
            datetime,
            constituent,
            "value",
            count
        FROM
            analytics.outlet_observations
        WHERE
            constituent NOT IN ('Q', 'QB')
    )

SELECT
    c.outlet_id,
    c.constituent,
    c.datetime,
    c."value",
    c.count,
    f.flow_value,
    b.baseflow_value
FROM
    constituent_data AS c
LEFT JOIN
    flow_data AS f
    ON c.outlet_id = f.outlet_id 
    AND c.datetime = f.datetime
LEFT JOIN
    baseflow_data AS b
    ON c.outlet_id = b.outlet_id 
    AND c.datetime = b.datetime;

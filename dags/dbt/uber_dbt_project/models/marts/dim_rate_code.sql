{{ config(
    materialized='table'
) }}
/*
    Rate Code Dimension:
    - Maps the RatecodeID to a descriptive rate code name.
    - Surrogate key 'rate_code_id' is set to the unique trip_id.
*/
WITH base AS (
    SELECT
        trip_id,
        RatecodeID
    FROM {{ ref('stg_uber_data') }}
)
SELECT
    trip_id AS rate_code_id,
    RatecodeID,
    CASE RatecodeID
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END AS rate_code_name
FROM base

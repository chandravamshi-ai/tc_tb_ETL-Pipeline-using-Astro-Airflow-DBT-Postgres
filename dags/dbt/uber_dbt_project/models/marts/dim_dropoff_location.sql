{{ config(
    materialized='table'
) }}
/*
    Dropoff Location Dimension:
    - Captures the dropoff location coordinates.
    - Surrogate key 'dropoff_location_id' is set to the unique trip_id.
*/
WITH base AS (
    SELECT
        trip_id,
        dropoff_longitude,
        dropoff_latitude
    FROM {{ ref('stg_uber_data') }}
)
SELECT
    trip_id AS dropoff_location_id,
    dropoff_longitude,
    dropoff_latitude
FROM base

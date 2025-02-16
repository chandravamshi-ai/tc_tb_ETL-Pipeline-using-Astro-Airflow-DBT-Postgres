{{ config(
    materialized='table'
) }}
/*
    Trip Distance Dimension:
    - Captures the trip_distance for each trip.
    - Surrogate key 'trip_distance_id' is set to the unique trip_id.
*/
WITH base AS (
    SELECT
        trip_id,
        trip_distance
    FROM {{ ref('stg_uber_data') }}
)
SELECT
    trip_id AS trip_distance_id,
    trip_distance
FROM base

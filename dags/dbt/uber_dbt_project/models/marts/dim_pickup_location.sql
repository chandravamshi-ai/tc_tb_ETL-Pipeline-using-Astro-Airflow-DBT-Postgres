{{ config(
    materialized='table'
) }}
/*
    Pickup Location Dimension:
    - Captures the pickup location coordinates.
    - Surrogate key 'pickup_location_id' is set to the unique trip_id.
*/
WITH base AS (
    SELECT
        trip_id,
        pickup_longitude,
        pickup_latitude
    FROM {{ ref('stg_uber_data') }}
)
SELECT
    trip_id AS pickup_location_id,
    pickup_longitude,
    pickup_latitude
FROM base

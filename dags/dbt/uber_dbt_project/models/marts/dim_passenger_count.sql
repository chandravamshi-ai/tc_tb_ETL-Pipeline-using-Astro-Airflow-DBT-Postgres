{{ config(
    materialized='table'
) }}
/*
    Passenger Count Dimension:
    - Captures the passenger_count for each trip.
    - Surrogate key 'passenger_count_id' is set to the unique trip_id.
*/
WITH base AS (
    SELECT
        trip_id,
        passenger_count
    FROM {{ ref('stg_uber_data') }}
)
SELECT
    trip_id AS passenger_count_id,
    passenger_count
FROM base

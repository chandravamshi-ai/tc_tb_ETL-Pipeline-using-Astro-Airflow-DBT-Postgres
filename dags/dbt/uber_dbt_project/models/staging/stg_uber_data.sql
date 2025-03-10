{{ config(
    materialized='view'
) }}

-- This staging model selects data from the raw source table 'uber_data'.
-- It serves as the base for our transformations.
SELECT DISTINCT ON (trip_id)
    trip_id,
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    RatecodeID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount
FROM {{ source('raw', 'uber_data') }}

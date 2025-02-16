{{ config(
    materialized='table'
) }}
/*
    Fact Trips Table:
    - Combines trip details with foreign keys referencing all dimensions.
    - Joins the staging data with each dimension using trip_id.
*/
WITH base AS (
    SELECT
        trip_id,
        VendorID,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    FROM {{ ref('stg_uber_data') }}
)
SELECT
    b.trip_id,
    b.VendorID,
    dtd.datetime_id,
    pcd.passenger_count_id,
    tdd.trip_distance_id,
    pld.pickup_location_id,
    dld.dropoff_location_id,
    rcd.rate_code_id,
    ptd.payment_type_id,
    b.fare_amount,
    b.extra,
    b.mta_tax,
    b.tip_amount,
    b.tolls_amount,
    b.improvement_surcharge,
    b.total_amount
FROM base b
LEFT JOIN {{ ref('dim_datetime') }} dtd ON b.trip_id = dtd.datetime_id
LEFT JOIN {{ ref('dim_passenger_count') }} pcd ON b.trip_id = pcd.passenger_count_id
LEFT JOIN {{ ref('dim_trip_distance') }} tdd ON b.trip_id = tdd.trip_distance_id
LEFT JOIN {{ ref('dim_rate_code') }} rcd ON b.trip_id = rcd.rate_code_id
LEFT JOIN {{ ref('dim_payment_type') }} ptd ON b.trip_id = ptd.payment_type_id
LEFT JOIN {{ ref('dim_pickup_location') }} pld ON b.trip_id = pld.pickup_location_id
LEFT JOIN {{ ref('dim_dropoff_location') }} dld ON b.trip_id = dld.dropoff_location_id

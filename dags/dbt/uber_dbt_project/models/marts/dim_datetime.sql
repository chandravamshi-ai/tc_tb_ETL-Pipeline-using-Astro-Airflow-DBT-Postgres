{{ config(
    materialized='table'
) }}
/*
    DateTime Dimension:
    - Extracts time-related attributes from pickup and dropoff timestamps.
    - Surrogate key 'datetime_id' is set to the unique trip_id.
*/
WITH base AS (
    SELECT
        trip_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime
    FROM {{ ref('stg_uber_data') }}
)
SELECT
    trip_id AS datetime_id,
    tpep_pickup_datetime,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
    EXTRACT(DAY FROM tpep_pickup_datetime) AS pickup_day,
    EXTRACT(DOW FROM tpep_pickup_datetime) AS pickup_weekday,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS pickup_month,
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS pickup_year,
    tpep_dropoff_datetime,
    EXTRACT(HOUR FROM tpep_dropoff_datetime) AS dropoff_hour,
    EXTRACT(DAY FROM tpep_dropoff_datetime) AS dropoff_day,
    EXTRACT(DOW FROM tpep_dropoff_datetime) AS dropoff_weekday,
    EXTRACT(MONTH FROM tpep_dropoff_datetime) AS dropoff_month,
    EXTRACT(YEAR FROM tpep_dropoff_datetime) AS dropoff_year
FROM base

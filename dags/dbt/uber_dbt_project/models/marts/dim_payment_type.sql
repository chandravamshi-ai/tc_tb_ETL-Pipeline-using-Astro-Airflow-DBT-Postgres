{{ config(
    materialized='table'
) }}
/*
    Payment Type Dimension:
    - Maps the payment_type code to a descriptive payment type name.
    - Surrogate key 'payment_type_id' is set to the unique trip_id.
*/
WITH base AS (
    SELECT
        trip_id,
        payment_type
    FROM {{ ref('stg_uber_data') }}
)
SELECT
    trip_id AS payment_type_id,
    payment_type,
    CASE payment_type
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Unknown'
    END AS payment_type_name
FROM base

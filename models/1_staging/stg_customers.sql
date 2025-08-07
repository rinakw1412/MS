{{
    config(
        materialized='view'
    )
}}
with base as (
    select 
        customer_id,
        first_name,
        last_name,
        to_date(date_of_birth, 'YYYY-MM-DD') as date_of_birth,
        address,
        city,
        province,
        ingestion_datetime
    from {{ source('financial_transaction', 'customers') }}
    where customer_id is not null
)   

select * from base
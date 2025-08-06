{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}
with base as (
    select 
        customer_id,
        first_name,
        last_name,
        date_of_birth,
        address,
        city,
        province
    from {{ source('financial_transaction', 'customers') }}
)

select * from base
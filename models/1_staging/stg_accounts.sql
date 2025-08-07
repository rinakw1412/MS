{{
    config(
        materialized='view'
    )
}}
with base as (
    select
        account_id,
        customer_id,
        to_date(opening_date, 'YYYY-MM-DD') as opening_date,
        coalesce(account_type,'unknown') as account_type,
        case 
            when balance = 'invalid' then 0
            when balance is null then 0
            else cast(balance as float)
        end as balanced,
        ingestion_datetime
    from {{ source('financial_transaction', 'accounts') }}
    where account_id is not null
)   

select * from base
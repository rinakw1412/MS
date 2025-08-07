{{
    config(
        materialized='view'
    )
}}
with base as (
    select 
        transaction_id,
        account_id,
        case 
          when transaction_date ~ '^\d{4}-\d{2}-\d{2}$'
          then to_date(transaction_date, 'YYYY-MM-DD')
          else null
        end as transaction_date,
        case 
            when amount is null then 0
            when amount = 'error' then 0
            else cast(amount as float)
        end as amount,
        currency,
        transaction_type,
        merchant_id,
        ingestion_datetime
      from {{ source('financial_transaction', 'transactions') }}
) ,
filtered as (
    select *
    from base
    where 
        transaction_id is not null 
        and account_id is not null
        and transaction_date is not null
        and currency is not null
),

addrownum as(
    select *,
    row_number() over ( 
        partition by transaction_id 
        order by ingestion_datetime desc
    ) as row_num
    from filtered
),

dedup as (
    select 
        transaction_id,
        account_id,
        transaction_date,
        amount,
        currency,
        transaction_type,
        merchant_id,
        ingestion_datetime
    from addrownum
    where row_num = 1
)

select * from dedup
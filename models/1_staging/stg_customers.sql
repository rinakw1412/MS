with base as (
    select *
    from {{ source('financial_transaction', 'customers') }}
    where 
        customer_id is not null
),

final as (
    select 
        customer_id,
        trim(lower(first_name)) as first_name,
        trim(lower(last_name)) as last_name,
        to_date(date_of_birth, 'YYYY-MM-DD') as date_of_birth,
        trim(lower(address)) as address,
        trim(lower(city)) as city,
        trim(lower(province)) as province,
        ingestion_datetime
    from base
)

select * from final
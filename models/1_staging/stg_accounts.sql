with 
base as (
    select a.*
    from {{ source('financial_transaction', 'accounts') }} a
    inner join {{ ref('stg_customers') }} c 
        on a.customer_id = c.customer_id
    where a.account_id is not null
),

final as (
    select 
        account_id,
        customer_id,
        to_date(opening_date, 'YYYY-MM-DD') as opening_date,
        lower(coalesce(account_type,'unknown')) as account_type,
        case 
            when nullif(trim(lower(balance)), '') = 'invalid' then 0
            when balance is null then 0
            else cast(nullif(trim(balance), '') as float)
        end as balance_usd,
        ingestion_datetime
    from base
)

select * from final
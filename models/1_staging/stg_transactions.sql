with base as (
    select 
        t.*
    from {{ source('financial_transaction', 'transactions') }} t
    inner join {{ ref('stg_accounts') }} a
        on t.account_id = a.account_id
    where 
        t.transaction_id is not null
    and t.currency is not null
) ,

clean as (
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
            when nullif(trim(lower(amount)), '') = 'error' then 0
            else cast(nullif(trim(amount), '') as float)
        end as amount,
        trim(lower(currency)) as currency,
        trim(lower(coalesce(transaction_type,'unknown'))) as transaction_type,
        merchant_id,
        ingestion_datetime
    from base        
),

filtered as(
    select *
    from clean 
    where  
        transaction_date is not null
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
    select * 
    from addrownum
    where row_num = 1
)

select * from dedup
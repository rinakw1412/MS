select 
    transaction_date,
    account_id,
    customer_id,
    transaction_type,
    count(transaction_id) as transaction_count,
    sum(coalesce(amount_usd, 0)) as total_amount_usd,
    avg(coalesce(amount_usd,0)) as avg_amount_usd
from {{ ref('int_transactions_accounts') }} 
group by 
    transaction_date,
    account_id,
    customer_id,
    transaction_type
    
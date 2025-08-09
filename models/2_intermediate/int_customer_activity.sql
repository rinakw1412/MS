select 
    ita.customer_id,
    count(distinct(ita.account_id)) as num_accounts, 
    count(ita.transaction_id) as total_txn_count,
    sum(amount_usd) as total_txn_amount_usd,
    min(ita.transaction_date) as first_txn_date,
    max(ita.transaction_date) as last_txn_date,
    count(distinct(ita.transaction_date)) as active_days
from 
    {{ ref('int_transactions_accounts') }} ita
join {{ ref('stg_customers') }} sc 
    on ita.customer_id = sc.customer_id
group by 1
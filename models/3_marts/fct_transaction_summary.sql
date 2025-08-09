select
    s.transaction_date,
    s.account_id,
    a.account_type,
    a.opening_date,
    a.balance_usd,
    s.customer_id,
    c.first_name,
    c.last_name,
    c.date_of_birth,
    c.address,
    c.city,
    c.province,
    s.transaction_type,
    s.transaction_count,
    s.total_amount_usd,
    s.avg_amount_usd
from {{ ref('int_transactions_daily_agg') }} s
left join {{ ref('dim_accounts') }} a
    on s.account_id = a.account_id
left join {{ ref('dim_customers') }} c
    on s.customer_id = c.customer_id
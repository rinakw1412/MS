select 
    sc.customer_id,
    sc.first_name,
    sc.last_name,
    sc.date_of_birth,
    sc.address,
    sc.city,
    sc.province,
    ica.num_accounts,
    ica.total_txn_count,
    ica.total_txn_amount_usd,
    ica.first_txn_date,
    ica.last_txn_date,
    ica.active_days

from {{ ref('stg_customers') }} sc
join {{ ref('int_customer_activity') }} ica
    on sc.customer_id = ica.customer_id
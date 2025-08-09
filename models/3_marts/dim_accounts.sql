select 
    account_id,
    customer_id,
    opening_date,
    account_type,
    balance_usd
from {{ ref('stg_accounts') }}
select
    ita.transaction_id,
    ita.account_id,
    ita.customer_id,
    ita.transaction_date,
    ita.transaction_type,
    ita.merchant_id,
    ita.amount_original,
    ita.currency,
    ita.conversion_rate,
    ita.amount_usd,
    sa.account_type,
    sa.opening_date,
    sa.balance_usd
from {{ ref('int_transactions_accounts') }} ita
left join {{ ref('dim_accounts') }} sa
    on ita.account_id = sa.account_id

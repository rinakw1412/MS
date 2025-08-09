with 
base as (
    select 
        st.transaction_id,
        st.account_id,
        sa.customer_id,
        st.transaction_date,
        st.transaction_type,
        st.amount as amount_original,
        st.currency,
        case
            when st.currency='IDR' then 0.000061
            when st.currency='EUR' then 1.16
            when st.currency='AUD' then 0.65
            when st.currency='JPY' then 0.0068
            else 1
        end as conversion_rate,
        st.merchant_id
    from {{ ref('stg_transactions')}} st
    left join {{ ref('stg_accounts') }} sa
        on st.account_id = sa.account_id
)

select *,
       amount_original * conversion_rate as amount_usd
from base
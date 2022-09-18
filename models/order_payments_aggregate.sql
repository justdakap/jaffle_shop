with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_payments_aggregate as (
    select
        order_id,
        min(created) as first_payment_date,
        max(created) as payment_finalized_date,
        sum(amount) as total_amount_paid,
        count(payment_id) as number_of_payments
    from
        payments
--    where
--        status <> 'fail'
    group by
        order_id
),

final as (
    select
        orders.*,
        order_payments_aggregate.first_payment_date,
        order_payments_aggregate.payment_finalized_date,
        order_payments_aggregate.total_amount_paid,
        order_payments_aggregate.number_of_payments
    from
        orders
    left join order_payments_aggregate
        using (order_id)
)


select
    *
from
    final
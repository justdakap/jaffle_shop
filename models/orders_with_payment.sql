with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_total_payments as (
    select
        order_id,
        max(created) as payment_finalized_date,
        sum(amount) as total_amount_paid
    from
        payments
    where
        status <> 'fail'
    group by
        order_id
),

orders_with_payment as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        order_total_payments.total_amount_paid,
        order_total_payments.payment_finalized_date
    from
        orders
        left join order_total_payments
            on orders.order_id = order_total_payments.order_id
),

final as (
    select
        *
    from
        orders_with_payment
)


select
    *
from
    final
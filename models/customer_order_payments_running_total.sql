with paid_orders as (
    select * from {{ ref('orders_with_payment') }}
)

 select
        p.order_id,
        p.order_placed_at,
        p.customer_id,
        SUM(p.total_amount_paid) OVER(PARTITION BY p.customer_id ORDER BY p.customer_id, p.order_id) as customer_order_payments_running_total
    from paid_orders p
order by p.order_id
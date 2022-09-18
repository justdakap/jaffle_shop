with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_order_aggregate as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from
        orders
    group by
        customer_id
),

paid_customer_orders as (
    select * from {{ ref('customer_order_payments_running_total') }}
)

select
paid_customer_orders.*,
ROW_NUMBER() OVER (ORDER BY paid_customer_orders.order_id) as transaction_seq,
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY paid_customer_orders.order_id) as customer_sales_seq,
CASE WHEN customer_order_aggregate.first_order_date = paid_customer_orders.order_placed_at
THEN 'new'
ELSE 'return' END as nvsr,
paid_customer_orders.customer_order_payments_running_total as customer_lifetime_value,
customer_order_aggregate.first_order_date as fdos
FROM paid_customer_orders
    LEFT JOIN customer_order_aggregate using (customer_id)
ORDER BY order_id
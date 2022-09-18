with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders_aggregate as (
    select * from {{ ref('customer_orders_aggregate') }}
),

order_payments_aggregate as (
    select * from {{ ref('order_payments_aggregate') }}
)

select
    order_id,
    customer_id,
    order_date as order_placed_at,
    status as order_status,
    total_amount_paid,
    payment_finalized_date,
    first_name as customer_first_name,
    last_name as customer_last_name,
    row_number() over (order by order_id) as transaction_seq,
    row_number() over (partition by customer_id order by order_id) as customer_sales_seq,
    case when first_order_date = order_date
    then 'new'
    else 'return' end as nvsr,
        SUM(total_amount_paid) OVER(PARTITION BY customer_id ORDER BY customer_id, order_id) as customer_lifetime_value,

-- x.clv_bad as customer_lifetime_value,
first_order_date as fdos

from
    customer_orders_aggregate
left join
    customers
    using (customer_id)
left join
    order_payments_aggregate
    using (customer_id)

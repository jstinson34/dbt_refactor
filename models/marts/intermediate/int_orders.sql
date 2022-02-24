With 

customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select * from {{ref('stg_jaffle_shop__orders')}}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

payment_total as (

select 

    orderid as order_id, 
    max(created) as payment_finalized_date, 
    sum(amount) / 100.0 as total_amount_paid

from payments
where status <> 'fail'
group by 1

),

paid_orders as (

select 
    orders.id as order_id,
    orders.user_id as customer_id,
    orders.order_date as order_placed_at,
    orders.status as order_status,
    payment_total.total_amount_paid,
    payment_total.payment_finalized_date,
    customers.first_name as customer_first_name,
    customers.last_name as customer_last_name

from orders

left join payment_total
on orders.id = payment_total.order_id

left join customers
on orders.user_id = customers.id 

)

select * from paid_orders



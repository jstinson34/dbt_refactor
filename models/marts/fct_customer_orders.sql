With 

customers as (
    select * from {{ ref('stg_jaffle_shop__customers')}}
),

orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

paid_orders as (
    select * from {{ ref('int_orders') }}
),

customer_orders as (
    
select customers.id as customer_id, 
    min(order_date) as first_order_date, 
    max(order_date) as most_recent_order_date,
    count(orders.id) as number_of_orders

from customers

left join orders
on orders.user_id = customers.id 

group by 1

),

total_clv as (

select

    p1.order_id,
    sum(p2.total_amount_paid) as clv_bad

from paid_orders p1

left join paid_orders p2 
on p1.customer_id = p2.customer_id and p1.order_id >= p2.order_id

group by 1
order by p1.order_id

),

--FINAL CTE

final as (

select

    p.*,
    row_number() over (order by p.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,
    case when customer_orders.first_order_date = p.order_placed_at
        then 'new' else 'return' end as nvsr,
    total_clv.clv_bad as customer_lifetime_value,
    customer_orders.first_order_date as fdos

from paid_orders as p

left join customer_orders using (customer_id)

left outer join total_clv 
on total_clv.order_id = p.order_id

order by order_id

)

select * from final

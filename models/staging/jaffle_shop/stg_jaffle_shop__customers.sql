With customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
)

select * from customers
With payments as (
    select * from {{ source('stripe', 'payment') }}
)

select * from payments
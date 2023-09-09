{{ 
    config(
        materialized='table'
    ) 
}}

with orders as (
    select * from {{ ref('fct_orders') }}
)

, final as (
    select 
        date_local
        , restaurant_name
        , count(1) as cnt_orders
        , countif(is_successful_order) as cnt_successful_orders
        , sum(cnt_products) as cnt_products
        , sum(case when is_successful_order then cnt_products else 0 end) as cnt_successful_products 

    from orders 
    group by 1,2
)

select * from final
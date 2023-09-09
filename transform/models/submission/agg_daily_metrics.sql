{{ 
    config(
        materialized='table'
    ) 
}}

with orders as (
    select * from {{ ref('fct_orders') }}
)

-- Total successful orders per day = sum(cnt_successful_orders)
-- Total successful orders per restaurant per day = cnt_successful_orders
-- Average number of products ordered per order per day = sum(cnt_products) / sum(cnt_orders)
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
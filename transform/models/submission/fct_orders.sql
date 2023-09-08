{{ 
    config(
        materialized='table'
    ) 
}}

with orders as (
    select * from {{ source('foodpanda_test', 'orders') }}
)

, restaurants as (
    select * from {{ source('foodpanda_test', 'vendors') }}
)

, orders_product_array as (
    select 
        *
        , split(product_id, ',') as products_array

    from orders
)

-- clean up product array
, final_orders as (
    select 
        * except (products_array)
        , array(
            select trim(product_id) from unnest(products_array) as product_id 
          ) as products

    from orders_product_array
) 

, final as (
    select 
        final_orders.date_local
        , final_orders.country_name
        , final_orders.customer_id
        , final_orders.gmv_local
        , final_orders.is_voucher_used
        , final_orders.is_successful_order
        , final_orders.products
        , restaurants.vendor_name as restaurant_name

    from final_orders 
    left join restaurants on final_orders.vendor_id = restaurants.id
)

select * from final
{{ 
    config(
        materialized='table'
    ) 
}}

with orders as (
    select * from {{ ref('fct_orders') }}
)

-- need timezone information for conversion of current_date (in UTC) to local time
-- assuming no records with date_local = current date 
, orders_l7d_flag as (
    select *
        , if(date_local >= date_add(current_date(), interval -7 day) and date_local < current_date(), True, False) as is_l7d

    from orders 
)

, snapshot_customers as (
    select 
        customer_id
        , count(1) as cnt_orders
        , countif(is_successful_order) as cnt_successful_orders

        , countif(is_l7d = false) as cnt_orders_before_l7d
        , countif(is_l7d = false and is_successful_order) as cnt_successful_orders_before_l7d

        , countif(is_l7d) as cnt_orders_l7d
        , countif(is_l7d and is_successful_order) as cnt_successful_orders_l7d

    from orders_l7d_flag 
    group by 1
)

-- assuming "reordered at least once in L7D" refers to customers who made at least 1 order BEFORE last 7 days AND WITHIN last 7 days
, final as (
    select 
        *
        , if(cnt_successful_orders > 0, True, False) as has_success_order
        , if(cnt_orders_before_l7d > 0 and cnt_orders_l7d > 0, True, False) as has_reorder_l7d

    from snapshot_customers
)

select * from final
{{ config(
    materialized = 'table',
    schema = 'retail_gold',
    alias = 'revenue_by_store',
    tags = ['gold', 'sales']
) }}

select
    s."Store_ID" as Store_ID,
    sum(s."Total_Amount") as total_revenue,
    sum(s."Quantity") as total_quantity
from {{ ref('silver_sales') }} s
group by
    s."Store_ID"
order by
    s."Store_ID"

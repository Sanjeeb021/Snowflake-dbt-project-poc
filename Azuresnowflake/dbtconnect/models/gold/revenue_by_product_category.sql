{{ config(
    materialized = 'table',
    schema = 'RETAIL_GOLD',
) }}

select
    s.Product_ID as ProductID,
    p.Category as Category,
    sum(s.Total_Amount) as Revenue
from {{ ref('silver_sales') }} s
join {{ ref('silver_products') }} p
    on s.Product_ID = p.Product_ID
group by
    s.Product_ID,
    p.Category
order by
    p.Category

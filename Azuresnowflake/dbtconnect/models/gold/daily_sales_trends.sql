{{ config(
    materialized = 'table',
    schema = 'RETAIL_GOLD',
    tags = ['gold', 'daily_sales_trends']
) }}

-- Step 1: Cast Total_Amount to FLOAT for correct aggregation
with sales_with_cast as (
    select
        "Store_ID",
        "Sales_Date",
        "Total_Amount",
        cast("Total_Amount" as float) as Total_Amount_Float
    from {{ ref('silver_sales') }}
)

-- Step 2: Aggregate daily totals per store
select
    "Store_ID" as StoreID,
    "Sales_Date",
    sum(Total_Amount_Float) as total_amount_sum
from sales_with_cast
group by
    "Store_ID",
    "Sales_Date"
order by
    "Store_ID",
    "Sales_Date"

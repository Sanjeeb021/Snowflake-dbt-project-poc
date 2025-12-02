{{ config(
    materialized = 'incremental',
    unique_key = '"Sale_ID"',
    tags = ['silver_layer', 'daily_refresh']
) }}

-- STEP 1: Load from Bronze with exact quoted names
with source as (
    select
        "Sale_ID",
        "Store_ID",
        "Product_ID",
        "Sale_Date",
        "Quantity",
        "Total_Amount"
    from {{ source('retail_bronze', 'sales') }}
),

-- STEP 2: Clean + Deduplicate
deduped as (
    select
        "Sale_ID",
        "Product_ID",
        "Store_ID",

        -- Cast Sale_Date safely
        TRY_TO_DATE("Sale_Date") as "Sales_Date",

        "Quantity",
        "Total_Amount"
    from source
    where "Sale_ID" is not null
      and "Product_ID" is not null
      and "Store_ID" is not null
      and "Quantity" > 0
      and "Total_Amount" > 0

    qualify row_number() over (
        partition by "Sale_ID"
        order by TRY_TO_DATE("Sale_Date") desc
    ) = 1
),

-- STEP 3: Join with product + store dimension tables
joined_data as (
    select
        d."Sale_ID",
        d."Product_ID",
        d."Store_ID",
        d."Sales_Date",
        d."Quantity",
        d."Total_Amount",

        p."Product_Name",
        p."Category" as "Product_Category",

        s."Store_Name",
        s."State" as "Store_Region"
    from deduped d
    inner join {{ ref('silver_products') }} p
        on d."Product_ID" = p."Product_ID"
    inner join {{ ref('silver_stores') }} s
        on d."Store_ID" = s."Store_ID"
)

-- STEP 4: Apply Incremental Filter
select *
from joined_data
{% if is_incremental() %}
  where "Sales_Date" > (select max("Sales_Date") from {{ this }})
{% endif %}

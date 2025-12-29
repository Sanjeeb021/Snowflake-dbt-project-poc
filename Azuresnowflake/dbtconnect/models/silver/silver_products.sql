{{ config(
    materialized='table',
    tags=['silver', 'products', 'cleaned']
) }}

with source as (
    select
        Product_ID,
        Product_Name,
        Category,
        Price
    from {{ source('retail_bronze', 'products') }}
),

-- Deduplicate rows based on Product_ID
deduped as (
    select
        Product_ID,
        Product_Name,
        Category,
        Price
    from (
        select
            *,
            row_number() over (
                partition by Product_ID
                order by Product_ID
            ) as rn
        from source
        where Product_ID is not null
          and Price > 0
    )
    where rn = 1
)

select * from deduped

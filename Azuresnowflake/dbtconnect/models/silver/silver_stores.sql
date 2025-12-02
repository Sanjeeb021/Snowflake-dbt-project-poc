{{ config(
    materialized = 'table',
    tags = ['silver', 'stores', 'cleaned']
) }}

-- Step 1: Read bronze table with exact quoted identifiers
with source as (
    select
        "Store_ID",
        "Store_Name",
        "State",
        "City"
    from {{ source('retail_bronze', 'stores') }}
),

-- Step 2: Deduplicate on Store_ID
deduped as (
    select
        "Store_ID",
        "Store_Name",
        "State",
        "City"
    from source
    where "Store_ID" is not null
    qualify row_number() over (
        partition by "Store_ID"
        order by "Store_ID"
    ) = 1
)

-- Step 3: Final output
select * from deduped

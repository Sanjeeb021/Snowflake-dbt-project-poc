-- ==========================================================================
-- Model: revenue_by_state
-- Description: Calculates revenue per store and groups by state.
-- ==========================================================================

{{ config(
    materialized = 'table',
    schema = 'RETAIL_GOLD',
) }}

select
    s."Store_ID" as Store_ID,
    st."State" as State,
    sum(s."Total_Amount") as Revenue
from {{ ref('silver_sales') }} s
join {{ ref('silver_stores') }} st
    on s."Store_ID" = st."Store_ID"
group by
    s."Store_ID",
    st."State"
order by
    st."State"

{% macro calculate_revenue(quantity_col, unit_price_col, total_amount_col=None) %}
    -- This macro calculates revenue based on the provided columns.
    -- Parameters:
    --   quantity_col: The column representing the quantity of items sold.
    --   unit_price_col: The column representing the unit price of the items.
    --   total_amount_col (optional): The column representing the pre-calculated total amount.
    -- If total_amount_col is provided, it uses its value (if not null) or falls back to calculating
    -- the revenue as quantity_col * unit_price_col. If total_amount_col is not provided, it directly
    -- calculates the revenue as quantity_col * unit_price_col.

    {% if total_amount_col %}
        -- Use total_amount_col if it is not null, otherwise calculate revenue as quantity * unit price.
        coalesce({{ total_amount_col }}, ({{ quantity_col }} * {{ unit_price_col }}))
    {% else %}
        -- Calculate revenue as quantity * unit price when total_amount_col is not provided.
        ({{ quantity_col }} * {{ unit_price_col }})
    {% endif %}
{% endmacro %}

{% macro show_new_rows_today(model_name) %}
    {% set sql %}
        SELECT COUNT(*) AS new_rows
        FROM {{ ref(model_name) }}
        WHERE "Sales_Date" = CURRENT_DATE()
    {% endset %}

    {% set result = run_query(sql) %}
    {% if execute %}
        {% set count = result.columns[0][0] %}
        {% do log("New rows loaded today: " ~ count, info=True) %}
    {% endif %}
{% endmacro %}

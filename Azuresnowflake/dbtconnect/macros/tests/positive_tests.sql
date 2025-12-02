{% test positive_quantity(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} <= 0
{% endtest %}

{% test positive_total_amount(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} <= 0
{% endtest %}

{% macro standardize_date(date_column) %}
  try_to_date({{ date_column }})
{% endmacro %}

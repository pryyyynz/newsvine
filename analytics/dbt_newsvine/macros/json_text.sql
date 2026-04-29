{% macro json_text(column_name, key_name) %}
  {{ return(adapter.dispatch('json_text', 'newsvine_phase5')(column_name, key_name)) }}
{% endmacro %}

{% macro newsvine_phase5__json_text(column_name, key_name) %}
  json_extract_string({{ column_name }}, '$.{{ key_name }}')
{% endmacro %}

{% macro postgres__json_text(column_name, key_name) %}
  {{ column_name }} ->> '{{ key_name }}'
{% endmacro %}

{% macro bigquery__json_text(column_name, key_name) %}
  JSON_VALUE({{ column_name }}, '$.{{ key_name }}')
{% endmacro %}

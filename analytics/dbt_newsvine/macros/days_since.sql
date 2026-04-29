{% macro days_since(ts_column) %}
  {{ return(adapter.dispatch('days_since', 'newsvine_phase5')(ts_column)) }}
{% endmacro %}

{% macro newsvine_phase5__days_since(ts_column) %}
  GREATEST(0, date_diff('second', {{ ts_column }}, current_timestamp) / 86400.0)
{% endmacro %}

{% macro postgres__days_since(ts_column) %}
  GREATEST(0, EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - {{ ts_column }})) / 86400.0)
{% endmacro %}

{% macro bigquery__days_since(ts_column) %}
  GREATEST(0, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), {{ ts_column }}, SECOND) / 86400.0)
{% endmacro %}

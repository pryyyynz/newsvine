{% macro float_type() %}
  {{ return(adapter.dispatch('float_type', 'newsvine_phase5')()) }}
{% endmacro %}

{% macro newsvine_phase5__float_type() %}
  double
{% endmacro %}

{% macro postgres__float_type() %}
  double precision
{% endmacro %}

{% macro bigquery__float_type() %}
  float64
{% endmacro %}

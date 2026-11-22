{% macro get_delta_path(table_name) %}
  delta_scan('{{ env_var("CLICK_STREAM_OUTPUT_PREFIX", "/opt/project/output_data") }}/{{ table_name }}')
{% endmacro %}

{% macro get_parquet_path(table_name) %}
  read_parquet('{{ env_var("CLICK_STREAM_OUTPUT_PREFIX", "/opt/project/output_data") }}/{{ table_name }}/*.parquet')
{% endmacro %}

{% macro get_stream_path(table_name) %}
  {{ get_parquet_path(table_name) }}
{% endmacro %}

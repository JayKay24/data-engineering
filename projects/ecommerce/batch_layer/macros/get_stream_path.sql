{% macro get_stream_path(table_name) %}
  read_parquet('{{ env_var("CLICK_STREAM_OUTPUT_PREFIX", "/opt/project/output_data") }}/{{ table_name }}/*.parquet')
{% endmacro %}

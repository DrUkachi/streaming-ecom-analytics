{#
    Use the configured schema name as-is (RAW, STAGING, REPORTING) instead of
    dbt's default "<target_schema>_<custom_schema>". This keeps the layered
    schemas described in the README and the table names Metabase already reads.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}

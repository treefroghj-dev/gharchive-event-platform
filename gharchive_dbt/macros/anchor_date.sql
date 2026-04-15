{% macro anchor_date() -%}
{# BigQuery DATE for "as-of" day: from `dbt run --vars '{"anchor_date": "YYYY-MM-DD"}'` (Airflow passes {{ ds }}); else current_date() for local runs. #}
{%- set v = var('anchor_date', none) -%}
{%- if v is not none and v | string | trim != '' -%}
date('{{ v }}')
{%- else -%}
current_date()
{%- endif -%}
{%- endmacro %}

{% macro flag_to_bool(column_name, true_value="Y", false_value="N", null_value=" ") -%}
(case
    when {{column_name}} = '{{true_value}}' then true
    when {{column_name}} = '{{false_value}}' then false
    when {{column_name}} = '{{null_value}}' then null
    when {{column_name}} is null then null
    else {{column_name}}
end)::bool
{%- endmacro %}
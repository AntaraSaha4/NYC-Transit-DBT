-- Macro to convert Y/N flag to Boolean 1/0
{% macro flag_to_boolean(flag_value) %}
    CASE {{flag_value}}
            WHEN 'Y' THEN '1'
            WHEN 'N' THEN '0'
    END
{% endmacro %}
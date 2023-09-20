{%- macro _get_column_list(model, transform="upper") -%}
{%- set relation_columns = adapter.get_columns_in_relation(model) -%}
{%- set relation_column_names = relation_columns | map(attribute="name") | map(transform) | list -%}
{%- do return(relation_column_names) -%}
{%- endmacro -%}

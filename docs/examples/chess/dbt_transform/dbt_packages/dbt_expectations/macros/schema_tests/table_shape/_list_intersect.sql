
{%- macro _list_intersect(list1, list2) -%}
{%- set matching_items = [] -%}
{%- for itm in list1 -%}
    {%- if itm in list2 -%}
        {%- do matching_items.append(itm) -%}
    {%- endif -%}
{%- endfor -%}
{%- do return(matching_items) -%}
{%- endmacro -%}

-- models/silver/lists.sql
{{
    config(
        materialization='incremental',
        incremental_strategy='insert_overwrite',
        loaded_at_field='loaded_at',
        schema='silver',
        tags='silver_layer',
    )
}}

with raw_lists as (
    select
        id,
        list_name,
        list_name_encoded,
        display_name,
        updated,
        list_image,
        list_image_width,
        list_image_height,
        row_number() OVER (PARTITION BY id ORDER BY updated DESC) as rn
    from {{ ref('raw_lists') }}
)
select
    id,
    list_name,
    list_name_encoded,
    display_name,
    updated,
    list_image,
    list_image_width,
    list_image_height
from raw_lists
where rn = 1

{% if is_incremental() %}

where updated > (select coalesce(max(updated), '1900-01-01') from {{ this }})

{% endif %}
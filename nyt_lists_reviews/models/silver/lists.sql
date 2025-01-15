-- models/silver/lists.sql

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
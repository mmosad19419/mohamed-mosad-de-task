-- models/silver/best_sellings_lists_books.sql

{{
    config(
        materialization="incremental",
        incremental_strategy="insert_overwrite",
        schema='silver',
        tags='silver_layer',
        loaded_at_field='loaded_at',
    )
}}

with raw_best_sellers as (
    select
        row_number() over () id,
        bestsellers_date,
        published_date,
        previous_published_date,
        next_published_date,
        list_id,
        book_id,
        rank,
        weeks_on_list,
        price
    from {{ ref('raw_best_sellings_lists_books') }}
)

select
    id,
    bestsellers_date,
    published_date,
    previous_published_date,
    next_published_date,
    list_id,
    book_id,
    rank,
    weeks_on_list,
    price
from raw_best_sellers

{% if is_incremental() %}

where published_date > (select coalesce(max(published_date), '1900-01-01') from {{ this }})

{% endif %}
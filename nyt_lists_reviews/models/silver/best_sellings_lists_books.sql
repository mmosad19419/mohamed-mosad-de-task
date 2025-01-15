-- models/silver/best_sellings_lists_books.sql

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

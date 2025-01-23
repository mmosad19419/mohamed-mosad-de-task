-- models/silver/books_buy_links.sql
{{
    config(
        materialization='incremental',
        incremental_strategy='insert_overwrite',
        unique_key=['book_id', 'website_name'],
        schema='silver',
        tags='silver_layer',
        loaded_at_field='loaded_at',
    )
}}

with raw_books_buy_links as (
    select
        book_id,
        website_name,
        website_url,
        row_number() OVER (PARTITION BY book_id, website_name) as rn
    from {{ ref('raw_books_buy_links') }}
)

select
    book_id,
    website_name,
    website_url
from raw_books_buy_links
where rn = 1

-- models/silver/books.sql

with raw_books as (
    select
        id,
        title,
        publisher,
        author,
        contributor,
        contributor_note,
        description,
        created_date,
        updated_date,
        age_group,
        amazon_product_url,
        primary_isbn13,
        primary_isbn10,
        book_image_width,
        book_image_height,
        first_chapter_link,
        book_uri,
        sunday_review_link,
        row_number() OVER (PARTITION BY id ORDER BY updated_date, created_date DESC) as rn
    from {{ ref('raw_books') }}
)

select
    id,
    title,
    publisher,
    author,
    contributor,
    contributor_note,
    description,
    created_date,
    updated_date,
    age_group,
    amazon_product_url,
    primary_isbn13,
    primary_isbn10,
    book_image_width,
    book_image_height,
    first_chapter_link,
    book_uri,
    sunday_review_link
from raw_books
where rn = 1

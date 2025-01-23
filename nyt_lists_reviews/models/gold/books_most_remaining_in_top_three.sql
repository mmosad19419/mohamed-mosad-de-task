{{
    config(
        materialization='view',
        schema='gold',
        unique_key=['book_id'],
        tags=['sql_question_01', 'gold_layer'],
    )
}}

WITH books_remaing_in_top_three AS (
    SELECT
        book_id,
        COUNT(*) AS weeks_in_top3
    FROM
       {{ ref('best_sellings_lists_books') }}
    WHERE
        rank <= 3
        AND EXTRACT(YEAR FROM published_date) = 2022
    GROUP BY
        book_id
)
SELECT
    b.title,
    b.author,
    t.book_id,
    t.weeks_in_top3
FROM
    books_remaing_in_top_three t
JOIN
    {{ref('books')}} b ON t.book_id = b.id
ORDER BY
    t.weeks_in_top3 DESC
LIMIT 1
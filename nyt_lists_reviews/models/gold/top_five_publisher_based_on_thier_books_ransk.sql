{{
    config(
        materialization='view',
        schema='gold',
        tags=['sql_question_3', 'gold_layer'],
    )
}}

WITH books_points AS (
    SELECT
        b.id,
        b.publisher,
        bp.rank,
        CASE
            WHEN bp.rank = 1 THEN 5
            WHEN bp.rank = 2 THEN 4
            WHEN bp.rank = 3 THEN 3
            WHEN bp.rank = 4 THEN 2
            WHEN bp.rank = 5 THEN 1
            ELSE 0
        END AS points,
        EXTRACT(YEAR FROM bp.published_date) AS year,
        EXTRACT(QUARTER FROM bp.published_date) AS quarter
    FROM
       {{ ref('best_sellings_lists_books') }} bp
    JOIN
        {{ ref('books') }} b ON bp.book_id = b.id
    WHERE
        bp.published_date BETWEEN '2021-01-01' AND '2023-12-31'
),
publisher_quartly_points AS (
    SELECT
        publisher,
        year,
        quarter,
        SUM(points) AS total_points
    FROM
        books_points
    GROUP BY
        publisher, year, quarter
),
publishers_ranks AS (
    SELECT
        publisher,
        year,
        quarter,
        total_points,
        RANK() OVER (PARTITION BY year, quarter ORDER BY total_points DESC) AS rank
    FROM
        publisher_quartly_points
)
SELECT
    publisher,
    year,
    quarter,
    total_points,
    rank
FROM
    publishers_ranks
WHERE
    rank <= 5
ORDER BY
    year, quarter, rank

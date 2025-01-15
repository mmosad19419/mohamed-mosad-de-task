WITH books_owned AS (
    SELECT
        bp.book_id,
        bp.list_id,
        bp.rank,
        b.title,
        b.publisher,
        bp.published_date,
        CASE
            WHEN bp.rank = 1 THEN 'Jake'
            WHEN bp.rank = 3 THEN 'Pete'
            ELSE NULL
        END AS team
    FROM
        silver.raw_best_sellings_lists_books bp
    JOIN
        silver.books b ON bp.book_id = b.id
    WHERE
        EXTRACT(YEAR FROM bp.published_date) = 2023
        AND bp.rank IN (1, 3)
),
books_shared AS (
    SELECT
        book_id,
        list_id,
        title,
        publisher,
        team,
        published_date,
        ROW_NUMBER() OVER (PARTITION BY list_id ORDER BY published_date) AS row_num
    FROM
        books_owned
)
SELECT
    book_id,
    list_id,
    title,
    publisher,
    team,
    published_date
FROM
    books_shared
WHERE
    row_num = 1
ORDER BY
    published_date, list_id, team

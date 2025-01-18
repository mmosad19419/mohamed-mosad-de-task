WITH lists_books_count AS (
    SELECT
        list_id,
        COUNT(DISTINCT book_id) AS unique_books_count
    FROM
       {{ ref('best_sellings_lists_books') }}
    GROUP BY
        list_id
)
SELECT
    l.list_name,
    l.id,
    lb.unique_books_count
FROM
    lists_books_count lb
JOIN
    dwh_silver.lists l ON lb.list_id = l.id
ORDER BY
    lb.unique_books_count ASC
LIMIT 3

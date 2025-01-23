SELECT *
FROM {{ ref('best_sellings_lists_books') }}
where price < 0
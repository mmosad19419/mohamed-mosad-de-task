CREATE TABLE dwh.fct_best_sellers_puplish (
  id integer PRIMARY KEY,
  bestsellers_date date,
  published_date date,
  previous_published_date date,
  next_published_date date,
  list_id varchar,
  book_id varchar,
  rank integer,
  weeks_on_list int,
  price integer
);

ALTER TABLE dwh.fct_best_sellers_puplish ADD FOREIGN KEY ("book_id") REFERENCES dwh.DimBooks ("id");

ALTER TABLE dwh.fct_best_sellers_puplish ADD FOREIGN KEY ("list_id") REFERENCES dwh.DimLists ("id");

ALTER TABLE dwh.fct_best_sellers_puplish ADD FOREIGN KEY ("published_date") REFERENCES dwh.DimDate ("FullDate");

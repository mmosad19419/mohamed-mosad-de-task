CREATE TABLE stage.best_sellers_puplish (
  id integer,
  bestsellers_date date,
  published_date date,
  previous_published_date date,
  next_published_date date,
  list_id varchar,
  book_id varchar,
  rank integer,
  weeks_on_list int,
  price numeric
);
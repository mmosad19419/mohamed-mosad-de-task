create schema bronze

CREATE TABLE bronze.books_buy_links (
  book_id varchar,
  website_name varchar,
  website_url text
);

CREATE TABLE bronze.books (
  id varchar,
  title varchar,
  publisher varchar,
  author varchar,
  contributor varchar,
  contributor_note text,
  description text,
  created_date date,
  updated_date date,
  age_group varchar,
  amazon_product_url text,
  primary_isbn13 varchar,
  primary_isbn10 varchar,
  book_image_width int,
  book_image_height int,
  first_chapter_link text,
  book_uri text,
  sunday_review_link text
);

CREATE TABLE bronze.lists (
  id varchar,
  list_name varchar,
  list_name_encoded varchar,
  display_name varchar,
  updated date,
  list_image text,
  list_image_width int,
  list_image_height int
);

CREATE TABLE bronze.best_sellings_lists_books (
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

GRANT USAGE ON SCHEMA bronze TO admin;
GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO admin;
CREATE TABLE silver.books_buy_links (
  book_id varchar,
  website_name varchar,
  website_url text,
  PRIMARY KEY ("book_id", "website_name")
);
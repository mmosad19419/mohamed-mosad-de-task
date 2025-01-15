CREATE TABLE dwh.DimBooks (
  id varchar PRIMARY KEY,
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


ALTER TABLE dwh.DimBooks ADD FOREIGN KEY (id) REFERENCES dwh.DimBooksBuyLinks (book_id);
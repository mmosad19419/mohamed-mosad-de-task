CREATE TABLE "dwh"."DimLists" (
  "id" varchar PRIMARY KEY,
  "list_name" varchar,
  "list_name_encoded" varchar,
  "display_name" varchar,
  "updated" date,
  "list_image" text,
  "list_image_width" int,
  "list_image_height" int
);
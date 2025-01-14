CREATE TABLE "dwh"."DimDate" (
  "DateKey" integer PRIMARY KEY,
  "FullDate" varchar,
  "DayOfMonth" varchar,
  "DayName" varchar,
  "DayOfWeek" varchar,
  "DayOfYear" varchar,
  "WeekOfYear" varchar,
  "Month" varchar,
  "MonthName" varchar,
  "Quarter" varchar,
  "Year" varchar,
  "MonthYear" varchar
);
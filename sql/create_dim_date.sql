CREATE TABLE dwh.DimDate (
  DateKey integer,
  FullDate date PRIMARY KEY,
  DayOfMonth varchar,
  DayName varchar,
  DayOfWeek varchar,
  DayOfYear varchar,
  WeekOfYear varchar,
  Month varchar,
  MonthName varchar,
  Quarter varchar,
  Year varchar,
  MonthYear varchar
);
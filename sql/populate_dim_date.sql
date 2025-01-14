WITH RECURSIVE date_series AS (
    SELECT '2010-01-01'::DATE AS Date
    UNION ALL
    SELECT Date + INTERVAL '1 day'
    FROM date_series
    WHERE Date < '2030-12-31'
)
INSERT INTO dwh.DimDate
    (
        DateKey,
        FullDate,
        DayOfMonth,
        DayName,
        DayOfWeek,
        DayOfYear,
        WeekOfYear,
        Month,
        MonthName,
        Quarter,
        QuarterName,
        Year,
        MonthYear
    )
SELECT 
    EXTRACT(YEAR FROM Date) * 10000 + EXTRACT(MONTH FROM Date) * 100 + EXTRACT(DAY FROM Date) AS DateKey,
    TO_CHAR(Date, 'dd-MM-yyyy') AS FullDate,
    TO_CHAR(Date, 'DD') AS DayOfMonth,
    TO_CHAR(Date, 'FMDay') AS DayName,
    EXTRACT(DOW FROM Date) + 1 AS DayOfWeek,  -- Day of the week starting from Sunday = 1
    EXTRACT(DOY FROM Date) AS DayOfYear,
    TO_CHAR(Date, 'IW') AS WeekOfYear,
    TO_CHAR(Date, 'MM') AS Month,
    TO_CHAR(Date, 'FMMonth') AS MonthName,
    CASE 
        WHEN EXTRACT(MONTH FROM Date) BETWEEN 1 AND 3 THEN '1'
        WHEN EXTRACT(MONTH FROM Date) BETWEEN 4 AND 6 THEN '2'
        WHEN EXTRACT(MONTH FROM Date) BETWEEN 7 AND 9 THEN '3'
        WHEN EXTRACT(MONTH FROM Date) BETWEEN 10 AND 12 THEN '4'
    END AS Quarter,
    TO_CHAR(Date, 'YYYY') AS Year,
    TO_CHAR(Date, 'YYYYMM') AS MonthYear
FROM date_series
;

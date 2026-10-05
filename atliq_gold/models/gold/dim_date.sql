SELECT
    d                                       AS date_day,
    CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
    year(d)                                 AS year,
    quarter(d)                              AS quarter,
    month(d)                                AS month,
    date_format(d, 'MMM')                   AS month_name,
    date_format(d, 'yyyy-MM')               AS year_month,
    dayofweek(d)                            AS day_of_week,
    date_format(d, 'EEE')                   AS weekday_name
FROM (
    SELECT explode(sequence(DATE'2024-01-01',
                            make_date(year(current_date()) + 1, 12, 31),
                            INTERVAL 1 DAY)) AS d
)
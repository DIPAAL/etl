CREATE TABLE
    dim_date (
        date_id serial PRIMARY KEY,
        date date,
        day_of_week int,
        day_of_month int,
        day_of_year int,
        week_of_year int,
        month_of_year int,
        quarter_of_year int,
        year int
        -- Padding: None, data and int are stored in 4 bytes. Serial are also int.
    );

-- Insert unknown date
INSERT INTO dim_date (date_id) VALUES (-1);

-- Insert all days since 2015 to 2026
INSERT INTO dim_date (date_id, date, day_of_week, day_of_month, day_of_year, week_of_year, month_of_year, quarter_of_year, year)
SELECT
    -- create smart id such that 2022-09-01 gets id 20220901
    (EXTRACT(YEAR FROM date) * 10000) + (EXTRACT(MONTH FROM date) * 100) + (EXTRACT(DAY FROM date)) AS date_id,
    date,
    EXTRACT(DOW FROM date),
    EXTRACT(DAY FROM date),
    EXTRACT(DOY FROM date),
    EXTRACT(WEEK FROM date),
    EXTRACT(MONTH FROM date),
    EXTRACT(QUARTER FROM date),
    EXTRACT(YEAR FROM date)
FROM
    generate_series('2015-01-01'::date, '2026-01-01'::date, '1 day') AS date;

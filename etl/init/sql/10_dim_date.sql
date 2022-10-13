    CREATE TABLE
        dim_date (
            date_id serial PRIMARY KEY,
            date date NOT NULL,
            day_of_week int NOT NULL,
            day_of_month int NOT NULL,
            day_of_year int NOT NULL,
            week_of_year int NOT NULL,
            month_of_year int NOT NULL,
            year int NOT NULL
        );

    -- Insert all days since 2005 to 2035
    INSERT INTO dim_date (date_id, date, day_of_week, day_of_month, day_of_year, week_of_year, month_of_year, year)
    SELECT
        -- create smart id such that 2022-09-01 gets id 20220901
        (EXTRACT(YEAR FROM date) * 10000) + (EXTRACT(MONTH FROM date) * 100) + (EXTRACT(DAY FROM date)) AS date_id,
        date,
        EXTRACT(DOW FROM date),
        EXTRACT(DAY FROM date),
        EXTRACT(DOY FROM date),
        EXTRACT(WEEK FROM date),
        EXTRACT(MONTH FROM date),
        EXTRACT(YEAR FROM date)
    FROM
        generate_series('2005-01-01'::date, '2035-01-01'::date, '1 day') AS date;

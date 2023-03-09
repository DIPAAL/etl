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
        year int,
        day_name TEXT,
        month_name TEXT,
        weekday TEXT
        -- Padding: None. All attributes are the same type.
    );

-- Insert unknown date
INSERT INTO dim_date (date_id) VALUES (-1);

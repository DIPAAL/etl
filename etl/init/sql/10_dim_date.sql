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
        -- 36 bytes = 4 bytes padding til 40 bytes
        day_name TEXT, 
        month_name TEXT,
        weekday TEXT,
        season TEXT,
        holiday TEXT
        -- 47 bytes = 1 bytes padding til 48 bytes
        -- Padding: 4 + 1 = 5 bytes padding
    );

-- Insert unknown date
INSERT INTO dim_date (date_id) VALUES (-1);

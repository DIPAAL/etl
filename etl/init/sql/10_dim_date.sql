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
        iso_year int,
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

-- Create 'dim_trajectory_start_date' role-playing view
CREATE OR REPLACE VIEW dim_trajectory_start_date
    (start_date_id, start_date, start_day_of_week, start_day_of_month, start_day_of_year, start_week_of_year, start_month_of_year, start_quarter_of_year, start_year, start_iso_year, start_day_name, start_month_name, start_weekday, start_season, start_holiday)
    AS SELECT date_id, date, day_of_week, day_of_month, day_of_year, week_of_year, month_of_year, quarter_of_year, year, iso_year, day_name, month_name, weekday, season, holiday FROM dim_date;

-- Create 'dim_trajectory_end_date' role-playing view
CREATE OR REPLACE VIEW dim_trajectory_end_date
    (end_date_id, end_date, end_day_of_week, end_day_of_month, end_day_of_year, end_week_of_year, end_month_of_year, end_quarter_of_year, end_year, end_iso_year, end_day_name, end_month_name, end_weekday, end_season, end_holiday)
    AS SELECT date_id, date, day_of_week, day_of_month, day_of_year, week_of_year, month_of_year, quarter_of_year, year, iso_year, day_name, month_name, weekday, season, holiday FROM dim_date;

-- Create 'dim_trajectory_eta_date' role-playing view
CREATE OR REPLACE VIEW dim_trajectory_eta_date
    (eta_date_id, eta_date, eta_day_of_week, eta_day_of_month, eta_day_of_year, eta_week_of_year, eta_month_of_year, eta_quarter_of_year, eta_year, eta_iso_year, eta_day_name, eta_month_name, eta_weekday, eta_season, eta_holiday)
    AS SELECT date_id, date, day_of_week, day_of_month, day_of_year, week_of_year, month_of_year, quarter_of_year, year, iso_year, day_name, month_name, weekday, season, holiday FROM dim_date;

-- Create 'dim_cell_entry_date' role-playing view
CREATE OR REPLACE VIEW dim_cell_entry_date
    (entry_date_id, entry_date, entry_day_of_week, entry_day_of_month, entry_day_of_year, entry_week_of_year, entry_month_of_year, entry_quarter_of_year, entry_year, entry_iso_year, entry_day_name, entry_month_name, entry_weekday, entry_season, entry_holiday)
    AS SELECT date_id, date, day_of_week, day_of_month, day_of_year, week_of_year, month_of_year, quarter_of_year, year, iso_year, day_name, month_name, weekday, season, holiday FROM dim_date;

-- Create 'dim_cell_exit_date' role-playing view
CREATE OR REPLACE VIEW dim_cell_exit_date
    (exit_date_id, exit_date, exit_day_of_week, exit_day_of_month, exit_day_of_year, exit_week_of_year, exit_month_of_year, exit_quarter_of_year, exit_year, exit_iso_year, exit_day_name, exit_month_name, exit_weekday, exit_season, exit_holiday)
    AS SELECT date_id, date, day_of_week, day_of_month, day_of_year, week_of_year, month_of_year, quarter_of_year, year, iso_year, day_name, month_name, weekday, season, holiday FROM dim_date;

CREATE TABLE
    dim_time (
        time time,
        time_id integer PRIMARY KEY,
        hour_of_day int,
        minute_of_hour int,
        second_of_minute int
        -- Padding: None. Aligned.
    );

-- Insert unknown time
INSERT INTO dim_time (time_id)
VALUES (-1);

-- Insert all seconds of a day
INSERT INTO dim_time (time_id, time, hour_of_day, minute_of_hour, second_of_minute)
SELECT
    -- create smart id such that 12:00:00 gets id 120000, note that 00:00:00 gets id 0, because leading 0s are removed.
    (EXTRACT(HOUR FROM time) * 10000) + (EXTRACT(MINUTE FROM time) * 100) + (EXTRACT(SECOND FROM time)) AS time_id,
    time,
    EXTRACT(HOUR FROM time),
    EXTRACT(MINUTE FROM time),
    EXTRACT(SECOND FROM time)
FROM
    generate_series('2022-01-01T00:00:00Z'::timestamp, '2022-01-01T23:59:59Z'::timestamp, '1 second') AS time;
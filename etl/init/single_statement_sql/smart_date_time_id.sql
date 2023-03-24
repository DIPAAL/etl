CREATE OR REPLACE FUNCTION timestamp_from_date_time_id(date_id INT, time_id INT) RETURNS TIMESTAMP AS $$
BEGIN
    -- parse as strings
    RETURN to_timestamp(
        concat(
            -- 0 pad date_id to 8 digits
            lpad(date_id::text, 8, '0'),
            -- 0 pad time_id to 6 digits
            lpad(time_id::text, 6, '0')
        ),
        'YYYYMMDDHH24MISS'
    );
END;
$$ language plpgsql IMMUTABLE;

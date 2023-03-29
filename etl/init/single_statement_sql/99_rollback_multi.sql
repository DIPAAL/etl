CREATE OR REPLACE FUNCTION dipaal_rollback(from_dateid INTEGER, to_dateid INTEGER, max_batch_size SMALLINT = 10) RETURNS void AS $$
DECLARE
    start_timestamp TIMESTAMP;
    end_timestamp TIMESTAMP;
    datetime RECORD;
    cur_month INTEGER;
    dateid_arr INTEGER[];
    cnt SMALLINT;
BEGIN
    -- Get timestamp versions of the start and end dates, timeid = 0 because it is not used
    start_timestamp := timestamp_from_date_time_id(from_dateid, 0);
    end_timestamp := timestamp_from_date_time_id(to_dateid, 0);

    -- Set initial values
    cur_month := 0;
    dateid_arr := ARRAY[]::INTEGER[];
    cnt := 0;
    FOR datetime IN SELECT
                        EXTRACT(YEAR FROM date) AS year,
                        EXTRACT(MONTH FROM date) AS month,
                        EXTRACT(DAY FROM date) AS day
                    FROM generate_series(start_timestamp, end_timestamp, '1 day'::interval) AS date
                    ORDER BY date ASC
    LOOP
        IF cur_month = 0 THEN
            cur_month = datetime.month;
        END IF;
        -- If we reach batch size or change month we clean and start batching again
        IF cnt >= max_batch_size OR cur_month != datetime.month THEN
            PERFORM dipaal_rollback(dateid_arr);
            cur_month := datetime.month;
            dateid_arr := ARRAY[(datetime.year * 10000) + (datetime.month * 100) + (datetime.day)];
            cnt := 1;
        ELSE
            dateid_arr := dateid_arr || ((datetime.year * 10000) + (datetime.month * 100) + (datetime.day));
            cnt := cnt + 1;
        END IF;
    END LOOP;
    -- If we have some left, we clean the rest
    IF array_upper(dateid_arr, 1) != 0 THEN
        PERFORM dipaal_rollback(dateid_arr);
    END IF;
END;
$$ LANGUAGE plpgsql;
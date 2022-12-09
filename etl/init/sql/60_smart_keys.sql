-- Create function to convert smart date such as 20220101 to date
CREATE OR REPLACE FUNCTION smart_date_to_date(smart_date integer)
RETURNS date AS $$
BEGIN
    RETURN to_date(lpad(smart_date::text, 8, '0', 'YYYYMMDD');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create function to convert smart time such as 120000 to time
CREATE OR REPLACE FUNCTION smart_time_to_time(smart_time integer)
RETURNS time AS $$
BEGIN
    -- zero
    RETURN to_time(lpad(smart_time::text, 6, '0'), 'HH24MISS');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create function to convert smart date and time such as 20220101 and 120000 to timestamp with time zone (UTC)
CREATE OR REPLACE FUNCTION smart_date_time_to_timestamp(smart_date integer, smart_time integer)
RETURNS timestamp with time zone AS $$
BEGIN
    -- zero pad the beginning to 8 digits
    RETURN to_timestamp(lpad(smart_date::text, 8, '0') || lpad(smart_time::text, 6, '0'), 'YYYYMMDDHH24MISS');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create function to convert timestamp to smart date
CREATE OR REPLACE FUNCTION timestamp_to_smart_date(timestamptz)
RETURNS integer AS $$
BEGIN
    RETURN to_char(timestamp, 'YYYYMMDD')::integer;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create function to convert timestamp to smart time
CREATE OR REPLACE FUNCTION timestamp_to_smart_time(timestamptz)
RETURNS integer AS $$
BEGIN
    RETURN to_char(timestamp, 'HH24MISS')::integer;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
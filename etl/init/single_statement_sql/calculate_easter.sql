-- Based on: https://www.rmg.co.uk/stories/topics/when-easter#:~:text=The%20simple%20standard%20definition%20of,Easter%20is%20the%20next%20Sunday.
-- Works for years between 1900 and 2099
CREATE OR REPLACE FUNCTION calculate_easter_holidays(cur_year SMALLINT) RETURNS TABLE (
    month SMALLINT,
    day SMALLINT
) AS $$
DECLARE
    D SMALLINT;
    E SMALLINT;
    Q SMALLINT;
    easter_month SMALLINT;
    month SMALLINT;
    offsets SMALLINT[];
    offs SMALLINT;
    day SMALLINT;
BEGIN
    -- The other easter holidays offset from easter sunday
    offsets := ARRAY[1, -2, -3, -7, 40];
    -- Calculate D
    D := 225 - 11 * (cur_year % 19);
    -- Subtract a multiple of 30 until D is less than 51
    WHILE D >= 51 LOOP
        D := D - 30;
    END LOOP;
    -- If D is greater than 48 subtract 1
    IF D > 48 THEN
        D := D - 1;
    END IF;
    -- Calculate E
    E := (cur_year + floor(cur_year/4)::int + D + 1) % 7;
    -- Calculate Q
    Q := D + 7 - E;
    -- If Q is less than 32 then Easter is in March
    IF Q < 32 THEN
        easter_month := 3;
    ELSE
        -- If Q is greater than 32 then Q -31 is its date in April
        easter_month := 4;
        Q := Q - 31;
    END IF;

    RETURN QUERY VALUES (easter_month, Q);
    FOR offs in SELECT * FROM UNNEST(offsets) LOOP
        day := Q + offs;
        month := easter_month;
        IF day > 31 THEN
            WHILE day > 31 LOOP
                day := day - 31;
                month := month + 1;
            END LOOP;
        ELSIF day < 1 THEN
            month := month -1;
            day := 31 + day;
        END IF;
        RETURN QUERY VALUES (month, day);
    END LOOP;
END;
$$ language plpgsql IMMUTABLE;
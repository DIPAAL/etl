CREATE SCHEMA IF NOT EXISTS staging;

-- Currently, only Danish holidays
CREATE TABLE IF NOT EXISTS staging.fixed_holidays (
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL
);

INSERT INTO staging.fixed_holidays (month, day) VALUES
    (1, 1), -- New Years Day 
    (5, 15), -- Whit Sunday (pinse)
    (5, 16), -- Whit Monday
    (6, 5), -- Constitution Day
    (12, 24), -- Christmas Eve
    (12, 25), -- Christmas Day
    (12, 26), -- 2nd Christmas Day
    (12, 31) -- New Years Eve
;

SELECT create_reference_table('staging.fixed_holidays');
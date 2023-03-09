CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS month_num_map (
    month_num SMALLINT PRIMARY KEY,
    month_name TEXT,
    season TEXT
);

INSERT INTO month_num_map (month_num, month_name, season) VALUES
    (1, 'january', 'winter'),
    (2, 'february', 'winter'),
    (3, 'march', 'spring'),
    (4, 'april', 'spring'),
    (5, 'may', 'spring'),
    (6, 'june', 'summer'),
    (7, 'july', 'summer'),
    (8, 'august', 'summer'),
    (9, 'september', 'autumn'),
    (10, 'october', 'autumn'),
    (11, 'november', 'autumn'),
    (12, 'december', 'winter');

SELECT create_reference_table('month_num_map');
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS month_num_to_month_name_map (
    month_num SMALLINT PRIMARY KEY,
    month_name TEXT
);

INSERT INTO month_num_to_month_name_map (month_num, month_name) VALUES
    (1, 'january'),
    (2, 'february'),
    (3, 'march'),
    (4, 'april'),
    (5, 'may'),
    (6, 'june'),
    (7, 'july'),
    (8, 'august'),
    (9, 'september'),
    (10, 'october'),
    (11, 'november'),
    (12, 'december');

SELECT create_reference_table('month_num_to_month_name_map');
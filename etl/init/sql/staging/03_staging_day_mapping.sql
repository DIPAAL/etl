CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS day_num_map (
    day_num SMALLINT PRIMARY KEY,
    day_name TEXT NOT NULL,
    weekday TEXT NOT NULL
);

INSERT INTO day_num_map (day_num, day_name, weekday) VALUES
    (0, 'sunday', 'no'),
    (1, 'monday', 'yes'),
    (2, 'tuesday', 'yes'),
    (3, 'wednesday', 'yes'),
    (4, 'thursday', 'yes'),
    (5, 'friday', 'yes'),
    (6, 'saturday', 'no');

SELECT create_reference_table('day_num_map');
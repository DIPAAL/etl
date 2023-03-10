CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS day_num_map (
    day_num SMALLINT PRIMARY KEY,
    day_name TEXT NOT NULL,
    weekday TEXT NOT NULL
);

INSERT INTO day_num_map (day_num, day_name, weekday) VALUES
    (0, 'sunday', 'weekend'),
    (1, 'monday', 'weekday'),
    (2, 'tuesday', 'weekday'),
    (3, 'wednesday', 'weekday'),
    (4, 'thursday', 'weekday'),
    (5, 'friday', 'weekday'),
    (6, 'saturday', 'weekend'),
    (7, 'sunday', 'weekend'); -- To also support 'ISODOW'

SELECT create_reference_table('day_num_map');
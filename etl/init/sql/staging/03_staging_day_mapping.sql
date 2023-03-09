CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS day_num_to_day_name_map (
    day_num SMALLINT PRIMARY KEY,
    day_name TEXT NOT NULL
);

INSERT INTO day_num_to_day_name_map (day_num, day_name) VALUES
    (0, 'sunday'),
    (1, 'monday'),
    (2, 'tuesday'),
    (3, 'wednesday'),
    (4, 'thursday'),
    (5, 'friday'),
    (6, 'saturday');

SELECT create_reference_table('day_num_to_day_name_map');
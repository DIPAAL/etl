CREATE TABLE dim_direction (
    direction_id smallserial PRIMARY KEY,
    "from" text NOT NULL, -- Between 4 and 6 bytes, as "West" is the shortest and "Unknown" is the longest
    -- Padding: Up to 3 bytes due to int alignment of text attributes.
    "to" text NOT NULL,
    -- Padding: None, as it is the last attribute.
    UNIQUE ("from", "to")


);

WITH directions as (
    SELECT
        unnest(ARRAY['North', 'South', 'East', 'West', 'Unknown']) as direction
)
INSERT INTO dim_direction ("from", "to")
SELECT
    from_direction.direction,
    to_direction.direction
FROM
    directions as from_direction,
    directions as to_direction;
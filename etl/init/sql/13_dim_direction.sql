CREATE TABLE dim_direction (
    direction_id smallserial PRIMARY KEY,
    "from" text NOT NULL,
    "to" text NOT NULL,
    UNIQUE ("from", "to")
    -- Padding: Can be measured given the direction is known.
    -- Not needed for now, as it would not change order currently.
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
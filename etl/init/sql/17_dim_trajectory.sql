CREATE TABLE dim_trajectory
(
    start_date_id integer NOT NULL,
    trajectory_sub_id integer NOT NULL,
    destination text NOT NULL,
    trajectory tgeompoint NOT NULL,
    heading tfloat,
    rot tfloat,
    draught tfloat,
    -- Padding: Some, but unknown. Text attribute will round up to nearest 4 bytes.
    -- MobilityDB types are variable length, but rounding is unknown.
    PRIMARY KEY (start_date_id, trajectory_sub_id)
) PARTITION BY RANGE (start_date_id);

-- Use gist as it is potentially overlappping.
CREATE INDEX dim_trajectory_trajectory_idx ON dim_trajectory USING gist (trajectory);


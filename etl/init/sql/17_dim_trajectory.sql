CREATE TABLE dim_trajectory
(
    date_id integer NOT NULL,
    trajectory_sub_id integer NOT NULL,
    trajectory tgeompoint NOT NULL,
    heading tfloat,
    rot tfloat,
    draught tfloat,
    destination text NOT NULL,
    PRIMARY KEY (date_id, trajectory_sub_id)
) PARTITION BY RANGE (date_id);

-- Use gist as it is potentially overlappping.
CREATE INDEX dim_trajectory_trajectory_idx ON dim_trajectory USING gist (trajectory);

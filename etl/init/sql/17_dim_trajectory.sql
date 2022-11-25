CREATE TABLE dim_trajectory
(
    date_id integer NOT NULL,
    trajectory_sub_id integer NOT NULL,
    trajectory tgeompoint NOT NULL,
    heading tfloat,
    rot tfloat,
    draught float,
    destination text NOT NULL,
    PRIMARY KEY (date_id, trajectory_sub_id)
) PARTITION BY RANGE (date_id);
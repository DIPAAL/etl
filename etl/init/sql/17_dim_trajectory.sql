CREATE TABLE dim_trajectory
(
    date_id integer NOT NULL,
    trajectory_id integer NOT NULL,
    trajectory tgeompoint NOT NULL,
    heading tfloat NOT NULL,
    rot tfloat NOT NULL,
    draught float NOT NULL,
    destination text NOT NULL,
    PRIMARY KEY (date_id, trajectory_id)
)
CREATE TABLE dim_trajectory
(
    trajectory_id SERIAL PRIMARY KEY,
    trajectory tgeompoint NOT NULL,
    heading tfloat NOT NULL,
    rot tfloat NOT NULL,
    draught float NOT NULL,
    destination text NOT NULL
)
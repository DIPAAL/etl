-- Create schema staging
CREATE SCHEMA IF NOT EXISTS staging;
-- Create a staging table for trajectories split into 5km.
CREATE TABLE IF NOT EXISTS staging.split_trajectories (
    trajectory_sub_id int,
    ship_id int,
    nav_status_id int,
    infer_stopped boolean,
    point geometry,
    trajectory tgeompoint,
    heading tfloat,
    draught tfloat,
    partition_id SMALLINT
);

SELECT create_distributed_table('staging.split_trajectories', 'trajectory_sub_id');
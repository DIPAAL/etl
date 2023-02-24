TRUNCATE staging.split_trajectories;

INSERT INTO staging.split_trajectories
SELECT
    t.trajectory_sub_id,
    t.ship_id,
    t.nav_status_id,
    t.infer_stopped,
    (t.split).point point,
    round(unnest(sequences((t.split).tpoint)), 3) trajectory, -- round to 1 milimeter, to avoid floating point errors
    t.heading,
    t.draught
FROM (
    SELECT
        ft.trajectory_sub_id, ft.ship_id, ft.nav_status_id, ft.infer_stopped,
        spaceSplit(transform(dt.trajectory, 3034), 5000) split,
        dt.heading heading, dt.draught draught
    FROM fact_trajectory ft
    JOIN dim_trajectory dt ON ft.trajectory_sub_id = dt.trajectory_sub_id AND ft.start_date_id = dt.date_id
    WHERE ft.start_date_id = %s) t;
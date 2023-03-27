TRUNCATE staging.split_trajectories;

INSERT INTO staging.split_trajectories
SELECT
    t2.*,
    sp.partition_id
FROM (
    SELECT
        t.trajectory_sub_id,
        t.ship_id,
        t.nav_status_id,
        t.infer_stopped,
        (t.split).point point,
        -- This forces lower and upper bound inclusiveness in the trajectory segments created by spaceSplit and sets linear interpolation between the points (lower, upper, linear)
        tgeompoint_seq(INSTANTS(ROUND(UNNEST(sequences((t.split).tpoint)), 3)), 'linear', true, true) AS trajectory, -- round to 1 milimeter, to avoid floating point errors
        t.heading,
        t.draught
    FROM (
        SELECT
            ft.trajectory_sub_id,
            ft.ship_id,
            ft.nav_status_id,
            ft.infer_stopped,
            spaceSplit(transform(dt.trajectory, 3034), 5000) split,
            dt.heading heading,
            dt.draught draught
        FROM fact_trajectory ft
        JOIN dim_trajectory dt ON ft.trajectory_sub_id = dt.trajectory_sub_id AND ft.start_date_id = dt.date_id
        WHERE ft.start_date_id = %s
    ) t
) t2
LEFT JOIN spatial_partition sp ON
    ST_Contains(sp.geom, t2.trajectory::geometry) OR
    ST_Covers(sp.geom, t2.trajectory::geometry) AND (
        (ST_XMin(sp.geom) = ST_XMin(t2.trajectory::geometry) AND ST_YMax(sp.geom) != ST_YMin(t2.trajectory::geometry)) OR
        (ST_YMin(sp.geom) = ST_YMin(t2.trajectory::geometry) AND ST_XMax(sp.geom) != ST_XMin(t2.trajectory::geometry))
    )
;

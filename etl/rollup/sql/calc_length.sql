UPDATE fact_trajectory ft
SET length = ST_Length(ST_Transform(dt.trajectory::geometry, 3034))::numeric(10,2)
FROM dim_trajectory dt
WHERE dt.trajectory_sub_id = ft.trajectory_sub_id AND dt.date_id = ft.start_date_id
    AND ft.start_date_id = %s;
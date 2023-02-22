UPDATE fact_trajectory ft
SET length = ROUND(ST_Length(ST_Transform(ST_SetSRID(dt.trajectory::geometry,4326), 3034)))::int
FROM dim_trajectory dt
WHERE dt.trajectory_sub_id = ft.trajectory_sub_id AND dt.date_id = ft.start_date_id
    AND ft.start_date_id = %s;
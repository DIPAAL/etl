UPDATE dim_trajectory dt
SET trajectory = transform(simplify(transform(setSRID(trajectory, 4326), 3034), 10, true), 4326)
FROM fact_trajectory ft
WHERE ft.trajectory_id = dt.trajectory_id AND
    ft.start_date_id = %s;
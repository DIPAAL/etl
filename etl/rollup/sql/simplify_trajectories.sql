UPDATE dim_trajectory dt
SET trajectory = transform(simplify(transform(trajectory, 3034), 10, true), 4326)
FROM fact_trajectory ft
WHERE ft.trajectory_sub_id = dt.trajectory_sub_id AND
      ft.start_date_id = dt.date_id AND
    ft.start_date_id = %s;
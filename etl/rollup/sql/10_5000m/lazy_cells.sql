INSERT INTO dim_cell_5000m (x, y, geom)
SELECT cell_x, cell_y, st_bounding_box::geometry
FROM fact_cell_5000m WHERE entry_date_id = 20220101
GROUP BY cell_x, cell_y, st_bounding_box::geometry
ON CONFLICT (x, y) DO NOTHING;
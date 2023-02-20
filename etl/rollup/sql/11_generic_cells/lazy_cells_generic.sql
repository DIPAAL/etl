INSERT INTO dim_cell_{CELL_SIZE} (x, y, parent_x, parent_y, geom)
SELECT cell_x, cell_y, cell_x/{CELL_DIVIDER}, cell_y/{CELL_DIVIDER}, st_bounding_box::geometry
FROM fact_cell_{CELL_SIZE} WHERE entry_date_id = %s
GROUP BY cell_x, cell_y, cell_x/4, cell_y/4, st_bounding_box::geometry
ON CONFLICT (x, y) DO NOTHING;
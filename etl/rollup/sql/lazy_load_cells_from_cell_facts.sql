WITH rows AS (
    INSERT INTO dim_cell_{CELL_SIZE}m (x, y, parent_x, parent_y, geom)
    SELECT cell_x, cell_y, {PARENT_FORMULA_X}, {PARENT_FORMULA_Y}, st_bounding_box::geometry
    FROM fact_cell_{CELL_SIZE}m WHERE entry_date_id = %s
    GROUP BY cell_x, cell_y, st_bounding_box::geometry
    ON CONFLICT (x, y) DO NOTHING
    RETURNING 1
)
SELECT count(*) FROM rows;
WITH rows AS (
    INSERT INTO dim_cell_{CELL_SIZE}m (x, y, parent_x, parent_y, geom, partition_id)
    SELECT cell_x, cell_y, {PARENT_FORMULA_X}, {PARENT_FORMULA_Y}, st_bounding_box::geometry, partition_id
    FROM fact_cell_{CELL_SIZE}m
    WHERE entry_date_id = %s
    GROUP BY partition_id, cell_x, cell_y, st_bounding_box::geometry
    ON CONFLICT (x, y, partition_id) DO NOTHING
    RETURNING 1
)
SELECT count(*) FROM rows;
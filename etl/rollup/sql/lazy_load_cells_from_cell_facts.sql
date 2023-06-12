WITH rows AS (
    INSERT INTO dim_cell_{CELL_SIZE}m (x, y, parent_x, parent_y, geom, division_id)
    SELECT cell_x, cell_y, {PARENT_FORMULA_X}, {PARENT_FORMULA_Y}, bounding_box::geometry, division_id
    FROM fact_cell_{CELL_SIZE}m
    WHERE entry_date_id = :date_smart_key
    GROUP BY division_id, cell_x, cell_y, bounding_box::geometry
    ON CONFLICT (x, y, division_id) DO NOTHING
    RETURNING 1
)
SELECT count(*) FROM rows;
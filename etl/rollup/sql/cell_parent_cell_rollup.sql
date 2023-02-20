INSERT INTO fact_cell_{PARENT_CELL_SIZE}
SELECT
    cell_x/{CELL_DIVIDER} AS cell_x,
    cell_y/{CELL_DIVIDER} AS cell_y,
    entry_date_id,
    ship_id,

FROM fact_cell_{CELL_SIZE} WHERE entry_date_id = %s
GROUP BY cell_x/{CELL_DIVIDER}, cell_y/{CELL_DIVIDER}, st_bounding_box::geometry
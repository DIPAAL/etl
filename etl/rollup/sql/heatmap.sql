INSERT INTO fact_cell_heatmap_{CELL_SIZE}m (
    cell_x, cell_y, date_id, ship_type_id, density_histogram
)
SELECT
    i1.cell_x,
    i1.cell_y,
    %(date_key)s AS date_id,
    i1.ship_type_id,
    (
        SELECT create_histogram(
            24, -- 24 hours = 24 entries in histogram
            ARRAY_AGG(i1.hour_of_day ORDER BY i1.hour_of_day ASC)::int[],
            ARRAY_AGG(i1.cnt ORDER BY i1.hour_of_day ASC)::int[]
        )
    ) AS density_histogram
FROM
    (
        SELECT
            fc.cell_x,
            fc.cell_y,
            ds.ship_type_id,
            dt.hour_of_day,
            COUNT (*) AS cnt
        FROM fact_cell fc
        INNER JOIN dim_time dt ON dt.time_id = fc.entry_time_id
        INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
        WHERE fc.entry_date_id = %(date_key)s
        GROUP BY fc.cell_x, fc.cell_y, dt.hour_of_day, ds.ship_type_id
    ) i1
GROUP BY i1.cell_x, i1.cell_y, i1.ship_type_id
;
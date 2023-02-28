-- Insert 5000m density heatmap
WITH raster_ref (rast) AS (
    SELECT ST_MakeEmptyRaster (795000, 420000, 3600000, 3055000, 1000, 1000, 0, 0, 3034)
)
INSERT INTO fact_cell_heatmap (cell_x, cell_y, date_id, time_id, ship_type_id, histogram_id, heatmap_type_id)
SELECT
    i2.cell_x,
    i2.cell_y,
    %(DATE_KEY)s AS date_id,
    (i2.hour_of_day || '0000')::int AS time_id,
    i2.ship_type_id,
    (
		SELECT
        	create_histogram(
            	i2.rast,
            	{CELL_SIZE},
            	3600
        	)
    ) AS histogram,
    (SELECT heatmap_type_id FROM dim_heatmap_type WHERE name = 'count') AS heatmap_type_id
FROM
    (
		SELECT
			ST_Union(
				ST_AsRaster(
					i1.geom,
				    rr.rast,
					'32BUI'::text,
					cnt::int
				)
			) AS rast,
			i1.cell_x / (5000 / {CELL_SIZE}) AS cell_x,
			i1.cell_y / (5000 / {CELL_SIZE}) AS cell_y,
			i1.hour_of_day,
			i1.ship_type_id
		FROM raster_ref rr,
		(
			SELECT
            	cell_x,
            	cell_y,
            	dt.hour_of_day,
            	ds.ship_type_id,
				dc.geom,
				COUNT(*) cnt
        	FROM fact_cell_{CELL_SIZE}m fc
        	INNER JOIN dim_time dt ON dt.time_id = fc.entry_time_id
        	INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
			INNER JOIN dim_cell_{CELL_SIZE}m dc ON dc.x = fc.cell_x and dc.y = fc.cell_y
        	WHERE fc.entry_date_id = %(DATE_KEY)s
        	GROUP BY fc.cell_x, fc.cell_y, dt.hour_of_day, ds.ship_type_id, dc.geom
    	) i1
		GROUP BY i1.cell_x / (5000 / {CELL_SIZE}), i1.cell_y / (5000 / {CELL_SIZE}), i1.hour_of_day, i1.ship_type_id
	) i2
;
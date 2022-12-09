-- Ship density raster in given time interval and spatial bound
WITH q_window(box, start_date_id, end_date_id) AS (
    SELECT
    STBox(
        ST_Transform(ST_MakeEnvelope(10.817894,57.164297, 11.287206, 57.376069, 4326),3034),
        period('2022-01-01 00:10:00+00', '2022-01-31 23:55:00+00')
    ) box,
    20220101 start_date_id,
    20220101 end_date_id
)
SELECT
	ST_AddBand(
			ST_MakeEmptyRaster(
				30000, --Width (Based on diff in q_window polygon)
				25000, --Height (Based on diff in q_window polygon)
				4047836.8218586454, --UpperLeft x (Long?)
				3379718.015347732, --UpperLeft y (Lat?)
				25, --Scale x
				25, --Scale y
				0, --Skew x
				0, --Skew y
				3034 --SRID
			), -- Raster
			(SELECT )
		)
	)
FROM (
	SELECT
		ST_Centroid(fc.st_bounding_box::geometry) AS center_point,
		COUNT(DISTINCT ds.*) AS cnt
	FROM q_window q
	INNER JOIN fact_cell fc ON fc.st_bounding_box && q.box
	INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
	GROUP BY fc.cell_x, fc.cell_y, fc.st_bounding_box::geometry
) AS grp;
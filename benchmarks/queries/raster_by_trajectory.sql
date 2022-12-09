-- Ship density raster in given time interval and spatial bound
WITH q_window(box, start_date_id, end_date_id) AS (
    SELECT
    STBox(
        ST_MakeEnvelope(10.817894,57.164297, 11.287206, 57.376069, 4326),
        period('2022-01-01 00:10:00+00', '2022-01-31 23:55:00+00')
    ) box,
    20220101 start_date_id,
    20220101 end_date_id
)

SELECT
	ST_Union(
		ST_AsRaster(
			split.point, --Geom
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
			), --Raster
			'8BSI'::text, --PixelType
			COUNT(*) --value
		)
	)
FROM (
    SELECT (spaceSplit(transform(atStBox(dt.trajectory, q_window.box), 3034), 50)).point
    FROM q_window
    INNER JOIN dim_trajectory dt ON atStBox(dt.trajectory, q_window.box) is not null
    INNER JOIN fact_trajectory fc ON fc.start_date_id = dt.date_id and fc.trajectory_sub_id = dt.trajectory_sub_id
) split
GROUP BY split.point
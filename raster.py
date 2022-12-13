import psycopg2

print("Connecting to database")
conn = psycopg2.connect(
    "dbname='ais' user='postgres' host='130.225.39.200' password='zZg0t3Rg9yYEbZ4iE9kJ' port='30036'")
print("Connected to database")
cur = conn.cursor()

sql = """WITH q_window(box, start_date_id, end_date_id) AS (
    SELECT
    SetSRID(STBox(

        ST_Transform(ST_MakeEnvelope(10.817894,57.164297, 11.287206, 57.376069, 4326),3034),
        period('2022-01-01 00:10:00+00', '2022-01-31 23:55:00+00')
    ),0) box,
    20220101 start_date_id,
    20220101 end_date_id
), raster_ref(rast) AS (
    SELECT ST_MakeEmptyRaster(
        30000,
        25000,
        4047836.8218586454,
        3379718.015347732,
        25,
        25,
        0,
        0,
        3034)
    )
SELECT
    ST_AsGDALRaster(
            ST_Union(
                ST_AsRaster(
                    grp.center_point,
                    raster_ref.rast,
                    '8BSI'::text,
                    grp.cnt
                )
            ),
            'GTiff'

    )
FROM raster_ref, (SELECT
		ST_SetSRID(ST_Centroid(fc.st_bounding_box::geometry),3034) AS center_point,
		COUNT(DISTINCT ds.*) AS cnt
	FROM q_window q
	INNER JOIN fact_cell fc ON fc.st_bounding_box && q.box
	INNER JOIN dim_ship ds ON ds.ship_id = fc.ship_id
	GROUP BY fc.cell_x, fc.cell_y, fc.st_bounding_box::geometry) AS grp
;"""
print("Executing query")
cur.execute(sql)
data = cur.fetchall()
print("Query executed")
tif_data = data[0][0]  # This can passed as the data variable in the requests.put function
with open("d:/gis/pg_raster.tif", "wb") as f:
    f.write(tif_data)

INSERT INTO fact_cell (
    cell_id, ship_id, ship_junk_id,
    entry_date_id, entry_time_id,
    exit_date_id, exit_time_id,
    direction_id, nav_status_id, trajectory_id,
    sog, cog, draught
)
SELECT
    cell_id,
    ship_id,
    ship_junk_id,
    (EXTRACT(YEAR FROM startTime) * 10000) + (EXTRACT(MONTH FROM startTime) * 100) + (EXTRACT(DAY FROM startTime)) AS entry_date_id,
    (EXTRACT(HOUR FROM startTime) * 10000) + (EXTRACT(MINUTE FROM startTime) * 100) + (EXTRACT(SECOND FROM startTime)) AS entry_time_id,
    (EXTRACT(YEAR FROM endTime) * 10000) + (EXTRACT(MONTH FROM endTime) * 100) + (EXTRACT(DAY FROM endTime)) AS exit_date_id,
    (EXTRACT(HOUR FROM endTime) * 10000) + (EXTRACT(MINUTE FROM endTime) * 100) + (EXTRACT(SECOND FROM endTime)) AS exit_time_id,
    (SELECT direction_id FROM dim_direction dd WHERE dd.from = entry_direction AND dd.to = exit_direction) AS direction_id,
    nav_status_id,
    trajectory_id,
    CASE
        WHEN durationSeconds = 0 THEN 0
        ELSE (length(crossing) / durationSeconds) * 1.94 -- 1 m/s = 1.94 knots
    END sog,
    0 cog,
    draught
FROM
(
    SELECT
        (
          SELECT key FROM json_each_text(start_edges)
          ORDER BY value::float ASC LIMIT 1
        ) as entry_direction,
        (
          SELECT key FROM json_each_text(end_edges)
          ORDER BY value::float ASC LIMIT 1
        ) as exit_direction,
        crossing,
        cell_id,
        ship_id,
        ship_junk_id,
        nav_status_id,
        trajectory_id,
        draught,
        atPeriod(heading, period(startTime, endTime, true, true)) heading,
        startTime,
        endTime,
        (EXTRACT(EPOCH FROM (endTime - startTime))) durationSeconds
    FROM
    (
        SELECT
            JSON_BUILD_OBJECT(
                'South', ST_Distance(startValue(crossing), south),
                'North', ST_Distance(startValue(crossing), north),
                'East', ST_Distance(startValue(crossing), east),
                'West', ST_Distance(startValue(crossing), west)
                ) AS start_edges,
            JSON_BUILD_OBJECT(
                'South', ST_Distance(endValue(crossing), south),
                'North', ST_Distance(endValue(crossing), north),
                'East', ST_Distance(endValue(crossing), east),
                'West', ST_Distance(endValue(crossing), west)
                ) AS end_edges,
            crossing,
            cell_id,
            ship_id,
            ship_junk_id,
            nav_status_id,
            trajectory_id,
            draught,
            heading,
            startTimestamp(crossing) startTime,
            endTimestamp(crossing) endTime
        FROM
        (
            SELECT
                unnest(sequences(atGeometry(transform(setsrid(dt.trajectory,4326),3034), dc.geom))) crossing,
                ST_SetSRID(ST_MakeLine(
                    ST_MakePoint(ST_XMin(dc.geom), ST_YMin(dc.geom)),
                    ST_MakePoint(ST_XMax(dc.geom), ST_YMin(dc.geom))
                ), 3034) south,
                ST_SetSRID(ST_MakeLine(
                    ST_MakePoint(ST_XMin(dc.geom), ST_YMin(dc.geom)),
                    ST_MakePoint(ST_XMin(dc.geom), ST_YMax(dc.geom))
                ), 3034) west,
                ST_SetSRID(ST_MakeLine(
                    ST_MakePoint(ST_XMax(dc.geom), ST_YMax(dc.geom)),
                    ST_MakePoint(ST_XMax(dc.geom), ST_YMin(dc.geom))
                ), 3034) east,
                ST_SetSRID(ST_MakeLine(
                    ST_MakePoint(ST_XMax(dc.geom), ST_YMax(dc.geom)),
                    ST_MakePoint(ST_XMin(dc.geom), ST_YMax(dc.geom))
                ), 3034) north,
                dc.cell_id cell_id,
                ft.ship_id ship_id,
                ft.ship_junk_id ship_junk_id,
                ft.nav_status_id nav_status_id,
                ft.trajectory_id trajectory_id,
                dt.draught draught,
                dt.heading heading
            FROM fact_trajectory ft
            JOIN dim_trajectory dt ON ft.trajectory_id = dt.trajectory_id
            JOIN dim_cell_50m dc ON ST_Intersects(dc.geom, transform(SetSRID(dt.trajectory,4326),3034)::geometry)
            WHERE duration > INTERVAL '1 second'
        ) TR1
    ) TR2
) TR3
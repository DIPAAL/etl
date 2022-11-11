INSERT INTO fact_cell (
    cell_x, cell_y, ship_id, ship_junk_id,
    entry_date_id, entry_time_id,
    exit_date_id, exit_time_id,
    direction_id, nav_status_id, trajectory_id,
    sog, delta_heading, draught
)
SELECT
    cell_x,
	cell_y,
    ship_id,
    ship_junk_id,
    (EXTRACT(YEAR FROM startTime) * 10000) + (EXTRACT(MONTH FROM startTime) * 100) + (EXTRACT(DAY FROM startTime)) AS entry_date_id,
    (EXTRACT(HOUR FROM startTime) * 10000) + (EXTRACT(MINUTE FROM startTime) * 100) + (EXTRACT(SECOND FROM startTime)) AS entry_time_id,
    (EXTRACT(YEAR FROM endTime) * 10000) + (EXTRACT(MONTH FROM endTime) * 100) + (EXTRACT(DAY FROM endTime)) AS exit_date_id,
    (EXTRACT(HOUR FROM endTime) * 10000) + (EXTRACT(MINUTE FROM endTime) * 100) + (EXTRACT(SECOND FROM endTime)) AS exit_time_id,
    (SELECT direction_id FROM dim_direction dd WHERE dd.from = entry_direction AND dd.to = exit_direction) AS direction_id,
    nav_status_id,
    trajectory_id,
    length(crossing) / GREATEST(durationSeconds, 1) * 1.94 sog, -- 1 m/s = 1.94 knots. Min 1 second to avoid division by zero
    0 delta_heading,
    draught
FROM (
        SELECT
            -- Select the JSON keys (north, south, east, west) with the lowest distance.
            (
              SELECT key FROM json_each_text(start_edges)
              ORDER BY value::float ASC LIMIT 1
            ) as entry_direction,
            (
              SELECT key FROM json_each_text(end_edges)
              ORDER BY value::float ASC LIMIT 1
            ) as exit_direction,
            crossing,
            cell_x,
            cell_y,
            ship_id,
            ship_junk_id,
            nav_status_id,
            trajectory_id,
            draught,
            atPeriod(heading, period(startTime, endTime, true, true)) heading,
            startTime,
            endTime,
            (EXTRACT(EPOCH FROM (endTime - startTime))) durationSeconds
        FROM (
            SELECT
                -- Construct the JSON objects existing of direction key, and distance to the cell edge
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
                cell_x,
                cell_y,
                ship_id,
                ship_junk_id,
                nav_status_id,
                trajectory_id,
                draught,
                heading,
                -- Truncate the entry and exit timestamp to second. Add almost a second to exit value, to be inclusive.
                date_trunc('second', startTimestamp(crossing)) startTime,
                date_trunc('second', endTimestamp(crossing) + INTERVAL '999999 microseconds') endTime
            FROM (
                SELECT
                    unnest(sequences(atGeometry(fdt.trajectory, dc.geom))) crossing,
                    -- Create the 4 lines representing the cell edges
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
                    dc.x cell_x,
                    dc.y cell_y,
                    fdt.ship_id ship_id,
                    fdt.ship_junk_id ship_junk_id,
                    fdt.nav_status_id nav_status_id,
                    fdt.trajectory_id trajectory_id,
                    fdt.draught draught,
                    fdt.heading heading
                FROM (
                    SELECT
                        ft.*,
                        -- Split the trajectory into cells of 2500m x 2500m. This makes it much faster to join to cell dimension.
                        (spaceSplit(transform(setSRID(dt.trajectory,4326),3034),2500)).tpoint point,
                        transform(setSRID(dt.trajectory, 4326), 3034) trajectory,
                        dt.heading heading,
                        dt.draught draught
                    FROM fact_trajectory ft
                    JOIN dim_trajectory dt ON ft.trajectory_id = dt.trajectory_id
                    WHERE duration > INTERVAL '1 second' AND ft.start_date_id = %s
                ) fdt
                JOIN dim_cell_50m dc ON ST_Intersects(dc.geom, fdt.point::geometry)
            ) ci
        ) cid
    ) cif
ON CONFLICT DO NOTHING;
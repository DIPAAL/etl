-- INSERT INTO fact_cell (
--     cell_x, cell_y, ship_id, ship_junk_id,
--     entry_date_id, entry_time_id,
--     exit_date_id, exit_time_id,
--     direction_id, nav_status_id, trajectory_sub_id,
--     sog, delta_heading, draught, delta_cog, st_bounding_box
-- )
SELECT
	test,
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
    trajectory_sub_id,
    length(crossing) / GREATEST(durationSeconds, 1) * 1.94 sog, -- 1 m/s = 1.94 knots. Min 1 second to avoid division by zero
    -- Calculate the delta between the elements in the headings array
	calculate_delta(headings),
    draught,
    delta_cog,
    stbox(cell_geom, period(startTime, endTime)) st_bounding_box
FROM (
        SELECT
            CASE
                WHEN south_entry < north_entry AND south_entry < east_entry AND south_entry < west_entry AND south_entry < threshold_distance_to_cell_edge THEN 'South'
WHEN north_entry < south_entry AND north_entry < east_entry AND north_entry < west_entry AND north_entry < threshold_distance_to_cell_edge THEN 'North'
                WHEN east_entry < south_entry AND east_entry < north_entry AND east_entry < west_entry AND east_entry < threshold_distance_to_cell_edge THEN 'East'
                WHEN west_entry < south_entry AND west_entry < north_entry AND west_entry < east_entry AND west_entry < threshold_distance_to_cell_edge THEN 'West'
                ELSE 'Unknown'
            END AS entry_direction,
            CASE
                WHEN south_exit < north_exit AND south_exit < east_exit AND south_exit < west_exit AND south_exit < threshold_distance_to_cell_edge THEN 'South'
WHEN north_exit < south_exit AND north_exit < east_exit AND north_exit < west_exit AND north_exit < threshold_distance_to_cell_edge THEN 'North'
                WHEN east_exit < south_exit AND east_exit < north_exit AND east_exit < west_exit AND east_exit < threshold_distance_to_cell_edge THEN 'East'
                WHEN west_exit < south_exit AND west_exit < north_exit AND west_exit < east_exit AND west_exit < threshold_distance_to_cell_edge THEN 'West'
                ELSE 'Unknown'
            END AS exit_direction,
			(SELECT ARRAY_AGG(LOWER(head)) FROM UNNEST(GETVALUES(atPeriod(heading, period(startTime, endTime, true, true)))) as head) as headings,
            crossing,
            cell_x,
            cell_y,
            cell_geom,
            ship_id,
            ship_junk_id,
            nav_status_id,
            trajectory_sub_id,
            draught,
            atPeriod(heading, period(startTime, endTime, true, true)) heading,
            startTime,
            endTime,
            delta_cog,
            (EXTRACT(EPOCH FROM (endTime - startTime))) durationSeconds
        FROM (
            SELECT
                ST_Distance(startValue(crossing), south) south_entry,
                ST_Distance(startValue(crossing), north) north_entry,
                ST_Distance(startValue(crossing), east) east_entry,
                ST_Distance(startValue(crossing), west) west_entry,
                ST_Distance(endValue(crossing), south) south_exit,
                ST_Distance(endValue(crossing), north) north_exit,
                ST_Distance(endValue(crossing), east) east_exit,
                ST_Distance(endValue(crossing), west) west_exit,
                threshold_distance_to_cell_edge,
                crossing,
                cell_x,
                cell_y,
                cell_geom,
                ship_id,
                ship_junk_id,
                nav_status_id,
                trajectory_sub_id,
                draught,
                heading,
                ( -- Calculate the Delta COG
                    SELECT SUM(ABS(LOWER(delta))) FROM UNNEST(GETVALUES(DEGREES(AZIMUTH(crossing)))) AS delta
                ) AS delta_cog,
                -- Truncate the entry and exit timestamp to second. Add almost a second to exit value, to be inclusive.
                date_trunc('second', startTimestamp(crossing)) startTime,
                date_trunc('second', endTimestamp(crossing) + INTERVAL '1000000 microseconds') endTime
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
                    0.2 threshold_distance_to_cell_edge,
                    dc.x cell_x,
                    dc.y cell_y,
                    dc.geom cell_geom,
                    fdt.ship_id ship_id,
                    fdt.ship_junk_id ship_junk_id,
                    fdt.nav_status_id nav_status_id,
                    fdt.trajectory_sub_id trajectory_sub_id,
                    fdt.draught draught,
                    fdt.heading heading
                FROM (
                    SELECT
                        ft.*,
                        -- Split the trajectory into cells of 2500m x 2500m. This makes it much faster to join to cell dimension.
                        (spaceSplit(transform(setSRID(dt.trajectory,4326),3034),2500)).tpoint point,
                        transform(dt.trajectory, 3034) trajectory,
                        dt.heading heading,
                        dt.draught draught
                    FROM fact_trajectory ft
                    JOIN dim_trajectory dt ON ft.trajectory_sub_id = dt.trajectory_sub_id AND ft.start_date_id = dt.date_id
                    WHERE duration > INTERVAL '1 second' AND ft.start_date_id = 20220101 AND ft.trajectory_sub_id = 1798538686
                ) fdt
                JOIN dim_cell_50m dc ON ST_Intersects(dc.geom, fdt.point::geometry)
            ) ci
        ) cid
    ) cif

INSERT INTO fact_cell_{CELL_SIZE}m (
    cell_x, cell_y, ship_id,
    entry_date_id, entry_time_id, exit_time_id,
    direction_id, nav_status_id, infer_stopped, trajectory_sub_id,
    sog, delta_heading, draught, delta_cog, st_bounding_box, partition_id
)
SELECT
    cell_x,
    cell_y,
    ship_id,
    (EXTRACT(YEAR FROM startTime) * 10000) + (EXTRACT(MONTH FROM startTime) * 100) + (EXTRACT(DAY FROM startTime)) AS entry_date_id,
    (EXTRACT(HOUR FROM startTime) * 10000) + (EXTRACT(MINUTE FROM startTime) * 100) + (EXTRACT(SECOND FROM startTime)) AS entry_time_id,
    (EXTRACT(HOUR FROM endTime) * 10000) + (EXTRACT(MINUTE FROM endTime) * 100) + (EXTRACT(SECOND FROM endTime)) AS exit_time_id,
    (SELECT direction_id FROM dim_direction dd WHERE dd.from = entry_direction AND dd.to = exit_direction) AS direction_id,
    nav_status_id,
    infer_stopped,
    trajectory_sub_id,
    length(crossing) / GREATEST (durationSeconds, 1) * 1.94 sog, -- 1 m/s = 1.94 knots. Min 1 second to avoid division by zero
    -- if delta_heading is null, then set as -1, else use calculate_delta
    -- upper_bound=360, as heading goes from 0 -> 359
    CASE WHEN heading IS NULL THEN
        -1
    ELSE
        calculate_delta_upperbounded ((
            SELECT
                ARRAY_AGG(LOWER(head))
            FROM UNNEST(GETVALUES (heading)) AS head), 360)
    END delta_heading,
    draught,
    delta_cog,
    stbox (cell_geom, crossing_period) st_bounding_box,
    partition_id
FROM (
    SELECT
        get_lowest_json_key (start_edges) entry_direction,
        get_lowest_json_key (end_edges) exit_direction,
        crossing,
        cell_x,
        cell_y,
        cell_geom,
        ship_id,
        nav_status_id,
        infer_stopped,
        trajectory_sub_id,
        startValue (atPeriod (draught, crossing_period)) draught,
        atPeriod (heading, crossing_period) heading,
        startTime,
        endTime,
        delta_cog,
        crossing_period,
        (EXTRACT(EPOCH FROM (endTime - startTime))) durationSeconds,
        partition_id
    FROM (
        SELECT
            *,
            period (cid.startTime, cid.endTime, TRUE, TRUE) crossing_period
        FROM (
            SELECT
                -- Construct the JSON objects existing of direction key, and distance to the cell edge
                JSON_BUILD_OBJECT(
                    'South', ST_Distance (startValue (crossing), south),
                    'North', ST_Distance (startValue (crossing), north),
                    'East', ST_Distance (startValue (crossing), east),
                    'West', ST_Distance (startValue (crossing), west),
                    'Unknown', threshold_distance_to_cell_edge
                    ) AS start_edges,
                JSON_BUILD_OBJECT(
                    'South', ST_Distance (endValue (crossing), south),
                    'North', ST_Distance (endValue (crossing), north),
                    'East', ST_Distance (endValue (crossing), east),
                    'West', ST_Distance (endValue (crossing), west),
                    'Unknown', threshold_distance_to_cell_edge
                    ) AS end_edges,
                crossing,
                cell_x,
                cell_y,
                cell_geom,
                ship_id,
                nav_status_id,
                infer_stopped,
                trajectory_sub_id,
                draught,
                heading,
                (
                    calculate_delta_upperbounded (
                        (
                            SELECT ARRAY_AGG(LOWER(delta))
                            FROM UNNEST(GETVALUES(DEGREES(AZIMUTH(crossing)))) AS delta
                        ),
                        360)
                ) AS delta_cog,
                -- Truncate the entry and exit timestamp to second.
                date_trunc('second', startTimestamp (crossing)) startTime,
                date_trunc('second', endTimestamp (crossing)) endTime,
                partition_id
            FROM (
                SELECT
                    unnest(sequences (atGeometry (fdt.trajectory, dc.geom))) crossing,
                    -- Create the 4 lines representing the cell edges
                    ST_SetSRID (
                        ST_MakeLine (
                            ST_MakePoint (ST_XMin (dc.geom), ST_YMin (dc.geom)),
                            ST_MakePoint (ST_XMax (dc.geom), ST_YMin (dc.geom))
                            ), 3034) south,
                    ST_SetSRID (
                        ST_MakeLine (
                            ST_MakePoint (ST_XMin (dc.geom), ST_YMin (dc.geom)),
                            ST_MakePoint (ST_XMin (dc.geom), ST_YMax (dc.geom))
                            ), 3034) west,
                    ST_SetSRID (
                        ST_MakeLine (
                            ST_MakePoint (ST_XMax (dc.geom), ST_YMax (dc.geom)),
                            ST_MakePoint (ST_XMax (dc.geom), ST_YMin (dc.geom))
                            ), 3034) east,
                    ST_SetSRID (
                        ST_MakeLine (
                            ST_MakePoint (ST_XMax (dc.geom), ST_YMax (dc.geom)),
                            ST_MakePoint (ST_XMin (dc.geom), ST_YMax (dc.geom))
                            ), 3034) north,
                    0.2 threshold_distance_to_cell_edge,
                    dc.x cell_x,
                    dc.y cell_y,
                    dc.geom cell_geom,
                    fdt.ship_id ship_id,
                    fdt.nav_status_id nav_status_id,
                    fdt.infer_stopped infer_stopped,
                    fdt.trajectory_sub_id trajectory_sub_id,
                    fdt.draught draught,
                    fdt.heading heading,
                    fdt.partition_id
                FROM staging.split_trajectories fdt
                JOIN staging.cell_{CELL_SIZE}m dc ON ST_Crosses(dc.geom, fdt.trajectory::geometry) OR ST_Contains(dc.geom, fdt.trajectory::geometry)
            ) cj
        ) cid
    ) cif
) ir
ON CONFLICT DO NOTHING;

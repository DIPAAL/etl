
SELECT
    i5.x,
    i5.y,
    i5.ship_id,
    i5.nav_status_id,
    i5.infer_stopped,
    i5.trajectory_sub_id,
    i5.draught,
    i5.delta_cog,
    (EXTRACT(YEAR FROM i5.start_time) * 10000) + (EXTRACT(MONTH FROM i5.start_time) * 100) + (EXTRACT(DAY FROM i5.start_time)) AS entry_date_id,
    (EXTRACT(HOUR FROM i5.start_time) * 10000) + (EXTRACT(MINUTE FROM i5.start_time) * 100) + (EXTRACT(SECOND FROM i5.start_time)) AS entry_time_id,
    (EXTRACT(YEAR FROM i5.end_time) * 10000) + (EXTRACT(MONTH FROM i5.end_time) * 100) + (EXTRACT(DAY FROM i5.end_time)) AS exit_date_id,
    (EXTRACT(HOUR FROM i5.end_time) * 10000) + (EXTRACT(MINUTE FROM i5.end_time) * 100) + (EXTRACT(SECOND FROM i5.end_time)) AS exit_time_id,
    (SELECT direction_id FROM dim_direction dd WHERE dd.from = i5.entry_direction AND dd.to = i5.exit_direction) AS direction_id,
    LENGTH(crossing) / GREATEST(i5.duration_seconds, 1) * 1.94 AS sog,
    CASE WHEN i5.heading IS NULL THEN
        -1
    ELSE
        calculate_delta_upperbounded((
            SELECT ARRAY_AGG(head)
            FROM UNNEST(GETVALUES(i5.heading)) AS head), 360)
    END AS delta_heading
FROM
(
    SELECT i4.crossing,
        i4.x,
        i4.y,
        i4.ship_id,
        i4.nav_status_id,
        i4.infer_stopped,
        i4.trajectory_sub_id,
        i4.start_time,
        i4.end_time,
        i4.delta_cog,
        i4.crossing_period,
        MinValue(AtTime(i4.draught, i4.crossing_period)) AS draught,
        AtTime(i4.heading, i4.crossing_period) AS heading,
        (EXTRACT(EPOCH FROM (i4.end_time - i4.start_time))) AS duration_seconds,
        get_lowest_json_key(i4.start_edges) AS entry_direction,
        get_lowest_json_key(i4.end_edges) AS exit_direction
    FROM 
    (
        SELECT 
            i3.*,
            SPAN(i3.start_time, i3.end_time, TRUE, TRUE) AS crossing_period
        FROM
        (
            SELECT
                i2.crossing,
                i2.x,
                i2.y,
                i2.cell_geom,
                i2.ship_id,
                i2.nav_status_id,
                i2.infer_stopped,
                i2.trajectory_sub_id,
                i2.draught,
                i2.heading,
                JSON_BUILD_OBJECT(
                    'South', ST_Distance(startValue(crossing), south),
                    'North', ST_Distance(startValue(crossing), north),
                    'East', ST_Distance(startValue(crossing), east),
                    'West', ST_Distance(startValue(crossing), west),
                    'Unknown', threshold_distance_to_cell_edge
                ) AS start_edges,
                JSON_BUILD_OBJECT(
                    'South', ST_Distance(endValue(crossing), south),
                    'North', ST_Distance(endValue(crossing), north),
                    'East', ST_Distance(endValue(crossing), east),
                    'West', ST_Distance(endValue(crossing), west),
                    'Unknown', threshold_distance_to_cell_edge
                ) AS end_edges,
                (
                    calculate_delta_upperbounded(
                        (
                            SELECT ARRAY_AGG(delta)
                            FROM UNNEST(GETVALUES(DEGREES(AZIMUTH(crossing)))) AS delta
                        ), 360)
                ) AS delta_cog,
                DATE_TRUNC('second', StartTimestamp(crossing)) AS start_time,
                DATE_TRUNC('second', EndTimestamp(crossing)) AS end_time
            FROM
            (
                SELECT
                    i1.trajectory_sub_id,
                    i1.ship_id,
                    i1.nav_status_id,
                    i1.infer_stopped,
                    i1.heading,
                    i1.draught,
                    i1.trajectory,
                    dc.x,
                    dc.y,
                    dc.geom AS cell_geom,
                    0.2 AS threshold_distance_to_cell_edge,
                    ST_SetSRID(
                        ST_Makeline(
                            ST_Makepoint(ST_XMax(dc.geom), ST_YMax(dc.geom)),
                            ST_Makepoint(ST_XMin(dc.geom), ST_YMax(dc.geom))
                        ), 3034
                    ) AS north,
                    ST_SetSRID(
                        ST_Makeline(
                            ST_Makepoint(ST_XMax(dc.geom), ST_YMax(dc.geom)),
                            ST_Makepoint(ST_XMax(dc.geom), ST_YMin(dc.geom))
                        ), 3034
                    ) AS east,
                    ST_SetSRID(
                        ST_Makeline(
                            ST_Makepoint(ST_XMin(dc.geom), ST_YMin(dc.geom)),
                            ST_Makepoint(ST_XMax(dc.geom), ST_YMin(dc.geom))
                        ), 3034
                    ) AS south,
                    ST_SetSRID(
                        ST_Makeline(
                            ST_Makepoint(ST_XMin(dc.geom), ST_YMin(dc.geom)),
                            ST_Makepoint(ST_XMin(dc.geom), ST_YMax(dc.geom))
                        ), 3034
                    ) AS west,
                    UNNEST(SEQUENCES(AtGeometry(i1.trajectory, dc.geom))) AS crossing
                FROM
                (
                    SELECT
                        ft.trajectory_sub_id,
                        ft.ship_id,
                        ft.nav_status_id,
                        ft.infer_stopped,
                        dt.heading,
                        dt.draught,
                        tgeompoint_seq(
                            INSTANTS(
                                ROUND(
                                    UNNEST(
                                        SEQUENCES(
                                            (SpaceSplit(TRANSFORM(dt.trajectory, 3034), 5000)).tpoint
                                        )
                                    ), 3
                                )
                            ), 'linear', true, true
                        ) AS trajectory
                    FROM fact_trajectory ft
                    INNER JOIN dim_trajectory dt ON ft.start_date_id = dt.date_id AND ft.trajectory_sub_id = dt.trajectory_sub_id
                    WHERE timestamp_from_date_time_id(ft.start_date_id, ft.start_time_id) >= timestamp_from_date_time_id(:start_date_id, :start_time_id)
                      AND timestamp_from_date_time_id(ft.start_date_id, ft.start_time_id) <= timestamp_from_date_time_id(:end_date_id, :end_time_id)
                      AND ST_Intersects(
                        ST_Makeenvelope(:xmin, :ymin, :xmax, :ymax, 3034),
                        ST_Transform(dt.trajectory::geometry, 3034)
                    )
                ) i1
                INNER JOIN staging.cell_50m dc ON (ST_Crosses(dc.geom, i1.trajectory::geometry) OR ST_Contains(dc.geom, i1.trajectory::geometry))
                                                    AND ST_Intersects(ST_Makeenvelope(:xmin, :ymin, :xmax, :ymax, 3034), dc.geom)
            ) i2
        ) i3
    ) i4
) i5
;
WITH
q_window(box) AS (
    SELECT
        stbox(
        st_makeenvelope(:xmin, :ymin, :xmax, :ymax, 3034),
        span(
            timestamp_from_date_time_id(:start_date, :start_time),
            timestamp_from_date_time_id(:end_date, :end_time),
            true, true
        )
    )
)
SELECT
    fc.cell_x,
    fc.cell_y,
    fc.ship_id,
    fc.entry_date_id,
    fc.entry_time_id,
    fc.exit_date_id,
    fc.exit_time_id,
    fc.direction_id,
    fc.nav_status_id,
    fc.infer_stopped,
    fc.trajectory_sub_id,
    fc.sog,
    fc.delta_heading,
    fc.draught,
    fc.delta_cog,
    fc.st_bounding_box
FROM q_window
JOIN fact_cell_50m fc ON fc.st_bounding_box && q_window.box
WHERE fc.entry_date_id BETWEEN :start_date AND :end_date
;
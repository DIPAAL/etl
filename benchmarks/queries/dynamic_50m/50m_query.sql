SELECT
    *
FROM fact_cell_50m fc
WHERE fc.st_bounding_box && stbox(
        st_makeenvelope(:xmin, :ymin, :xmax, :ymax, 3034),
        span(
            timestamp_from_date_time_id(:start_date, :start_time),
            timestamp_from_date_time_id(:end_date, :end_time),
            true, true
        )
    )
;
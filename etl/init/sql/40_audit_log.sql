CREATE TABLE audit_log (
    audit_id serial PRIMARY KEY,
    import_date integer,
    import_time integer,

    file_imported text,
    file_size integer,
    etl_version_major integer,
    etl_version_middle integer,
    etl_version_minor integer,

    -- The following fields store the time it took to perform each step of the ETL process.
    timespan_initial_cleaning integer,
    timespan_spatial_join integer,
    timespan_trajectory_construction integer,
    timespan_bulk_insert integer,
    timespan_total integer,

    -- The following fields store the number of rows after the data has been processed by each step of the ETL process.
    rows_imported integer, -- The number of rows in the input file.
    rows_initial_cleaning integer,
    rows_spatial_join integer,
    rows_trajectory_construction integer,
    rows_bulk_insert integer
)
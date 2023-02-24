CREATE TABLE audit_log (
    file_size bigint,
    import_datetime timestamp,
    audit_id serial PRIMARY KEY,
    file_rows integer,
    total_delta_time integer,
    -- Used to store information such as runtime and rows of individual steps.
    debug_info jsonb,
    etl_version text,
    file_name text,
    requirements text[],
 )
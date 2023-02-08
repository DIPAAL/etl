CREATE TABLE audit_log (
    file_size bigint, -- 8 bytes
    import_datetime timestamp, -- Aligned
    audit_id serial PRIMARY KEY,
    file_rows integer, -- Aligned

   -- Delta time is the time each stage took to complete in seconds
    -- Row count is the number of rows after being processed by each stage with the exception of bulk insert
    total_delta_time integer,

    cleaning_delta_time integer, -- Aligned
    cleaning_rows integer,

    spatial_join_delta_time integer, -- Aligned
    spatial_join_rows integer,

    trajectory_delta_time integer, -- Aligned
    trajectory_rows integer,

    cell_construct_delta_time integer, -- Aligned
    cell_construct_rows integer,

    bulk_insert_delta_time integer, -- Aligned
    -- Padding: None

    etl_version text,
    file_name text,
    requirements text[],
    bulk_insert_insertion_stats jsonb
    -- Padding: Some, at worst 9 bytes. Text attributes will round up to nearest 4 bytes.
 )
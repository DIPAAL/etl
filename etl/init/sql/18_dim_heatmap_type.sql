CREATE TABLE IF NOT EXISTS dim_heatmap_type (
    heatmap_type_id SMALLSERIAL PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    name text NOT NULL,
    description text,
    union_type text NOT NULL DEFAULT 'SUM', -- see https://postgis.net/docs/RT_ST_Union.html
    pixel_type text NOT NULL DEFAULT '32BSI' -- see https://postgis.net/docs/RT_ST_BandPixelType.html
);

INSERT INTO dim_heatmap_type (slug, name, description, union_type, pixel_type) VALUES
    (
        'count',
        'Count',
        'Count of trajectories crossing the cell',
        'SUM',
        '32BSI'
    ),
    (
        'delta_cog',
        'Delta Course Over Ground',
        'The average change in course over ground of trajectories crossing the cell',
        'SUM',
        '32BF'
    ),
    (
        'delta_heading',
        'Delta Heading',
        'The average change in heading of trajectories crossing the cell',
        'SUM',
        '32BF'
    ),
    (
        'max_draught',
        'Maximum Draught',
        'The maximum draught of trajectories crossing the cell',
        'MAX',
        '32BF'
    ),
    (
        'time',
        'Time',
        'The sum of time spent in the cell by trajectories crossing the cell (as seconds)',
        'SUM',
        '32BSI'
    )
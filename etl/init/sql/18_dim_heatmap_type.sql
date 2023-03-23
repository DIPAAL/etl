CREATE TABLE IF NOT EXISTS dim_heatmap_type (
    heatmap_type_id SMALLSERIAL PRIMARY KEY,
    slug text NOT NULL UNIQUE,
    name text NOT NULL,
    description text
);

INSERT INTO dim_heatmap_type (slug, name, description) VALUES
(
    'count',
    'Count',
    'Count of trajectories crossing the cell'
);
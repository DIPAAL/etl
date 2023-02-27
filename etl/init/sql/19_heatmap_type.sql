CREATE TABLE IF NOT EXISTS dim_heatmap_type (
    heatmap_type_id SMALLSERIAL PRIMARY KEY,
    name text NOT NULL
);

INSERT INTO dim_heatmap_type (name) VALUES ('unknown');
INSERT INTO dim_heatmap_type (name) VALUES ('count');
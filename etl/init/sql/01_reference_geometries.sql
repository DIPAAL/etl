CREATE TABLE IF NOT EXISTS reference_geometries(
    id SERIAL PRIMARY KEY,
    name text NOT NULL,
    type text NOT NULL,
    geom GEOMETRY,
    geom_geodetic GEOMETRY NOT NULL
);
CREATE TABLE IF NOT EXISTS reference_geometries(
    id SERIAL PRIMARY KEY,
    name text NOT NULL,
    type text NOT NULL,
    geom GEOMETRY,
    geom_geodetic GEOMETRY NOT NULL
);

-- The rectangle specifying the spatial domain of DIPAAL.
WITH spatial_domain(geom) AS (
    SELECT
        ST_MakeEnvelope(
            3480000,
            2930000,
            4495000,
            3645000,
            3034
        ) AS geom
)
INSERT INTO reference_geometries (name, type, geom, geom_geodetic)
SELECT
    'danish_waters_spatial_domain',
    'spatial_domain',
    geom,
    ST_Transform(geom, 4326)
FROM spatial_domain;

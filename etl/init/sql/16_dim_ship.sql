CREATE TABLE dim_ship (
    a float,
    b float,
    c float,
    d float,
    width float, -- = C+D
    length float, -- = A+B
    ship_id serial PRIMARY KEY,
    imo int NOT NULL,
    mmsi int NOT NULL,
    mid SMALLINT NOT NULL,
    ship_type_id SMALLINT NOT NULL,
    -- Padding: 8-4 = 4 bytes
    name text,
    callsign text,
    location_system_type TEXT NOT NULL,
    flag_region TEXT NOT NULL,
    flag_state TEXT NOT NULL,
    UNIQUE (imo, mmsi, name, callsign, a, b, c, d, ship_type_id, location_system_type),
    FOREIGN KEY (ship_type_id) REFERENCES dim_ship_type(ship_type_id)
    -- Padding: Undeterminable as "name" and "callsign" can be whatever they want to enter and an upper bound is therefore not possible to establish.
);

-- Create index on mmsi
CREATE INDEX dim_ship_mmsi_idx ON dim_ship (mmsi);

-- Create index on imo
CREATE INDEX dim_ship_imo_idx ON dim_ship (imo);

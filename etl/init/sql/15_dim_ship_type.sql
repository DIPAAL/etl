CREATE TABLE IF NOT EXISTS dim_ship_type (
    ship_type_id SMALLSERIAL PRIMARY KEY,
    mobile_type TEXT NOT NULL,
    ship_type TEXT NOT NULL,

    UNIQUE(mobile_type, ship_type)
)
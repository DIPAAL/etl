CREATE TABLE dim_nav_status (
    nav_status_id smallserial PRIMARY KEY,
    nav_status text NOT NULL UNIQUE
    -- Padding: None, as variable length attribute is last.
);

-- Insert Unknown status
INSERT INTO dim_nav_status (nav_status_id, nav_status)
VALUES (0, 'Unknown');

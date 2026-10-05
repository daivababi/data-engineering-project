-- A few sample dates for testing
INSERT INTO dim_date (full_date, day_of_week) VALUES
    ('2025-10-06', 'Monday'),
    ('2025-10-07', 'Tuesday'),
    ('2025-10-08', 'Wednesday'),
    ('2025-10-09', 'Thursday'),
    ('2025-10-10', 'Friday');

-- ---------- dim_stop ----------
-- Sample bus stops in Tartu (name + coordinates)
INSERT INTO dim_stop (stop_name, latitude, longitude) VALUES
    ('Vabaduse puiestee', 58.377700, 26.729100),
    ('Kesklinn',          58.380900, 26.722500),
    ('Ropka',             58.362400, 26.718000),
    ('Annelinn',          58.375400, 26.755000),
    ('Turu',              58.380100, 26.725900);

-- ---------- dim_trip ----------
-- Sample scheduled trips
INSERT INTO dim_trip (stop_key, line_no, route, scheduled_arrival) VALUES
    (1, '1',  'Vabaduse - Kesklinn',  '2025-10-06 08:15:00'),
    (2, '1',  'Kesklinn - Ropka',     '2025-10-06 08:30:00'),
    (3, '4',  'Ropka - Annelinn',     '2025-10-06 09:00:00'),
    (4, '12', 'Annelinn - Kesklinn',  '2025-10-07 17:45:00'),
    (5, '12', 'Kesklinn - Turu',      '2025-10-07 18:00:00');

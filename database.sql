CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    total_seats INTEGER NOT NULL CHECK (total_seats > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bookings (
    id SERIAL PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(event_id, user_id)
);

-- UNIQUE(event_id, user_id) уже создаёт составной btree-индекс (event_id, user_id).
-- По правилу leftmost-prefix он ускоряет и запросы "WHERE event_id = ...",
-- поэтому отдельный idx_bookings_event_user был избыточен и удалён.
--
-- Для запроса "все брони конкретного пользователя" нужен отдельный индекс,
-- так как user_id не является самым левым столбцом составного индекса.
CREATE INDEX idx_bookings_user_id ON bookings(user_id);

INSERT INTO events (name, total_seats) VALUES 
('Концерт рок-группы', 100),
('Театральная премьера', 50);
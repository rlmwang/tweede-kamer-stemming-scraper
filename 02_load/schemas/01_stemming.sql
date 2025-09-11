CREATE TABLE IF NOT EXISTS stemming (
    stemming_id TEXT PRIMARY KEY,
    stemming_did TEXT NOT NULL,
    titel TEXT,
    datum DATE,
    type TEXT
);

CREATE TABLE indieners (
    motie_id TEXT NOT NULL,
    stemming_id TEXT NOT NULL,
    name TEXT,
    type TEXT,
    PRIMARY KEY (motie_id, stemming_id, name),
    FOREIGN KEY (motie_id, stemming_id) REFERENCES motie(motie_id, stemming_id)
);

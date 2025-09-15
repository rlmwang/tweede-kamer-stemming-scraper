CREATE TABLE details (
    motie_id TEXT NOT NULL,
    stemming_id TEXT NOT NULL,
    fractie TEXT,
    kamerlid TEXT DEFAULT 'nvt',
    zetels TEXT,
    stem TEXT,
    niet_deelgenomen TEXT,
    vergissing BOOLEAN,
    PRIMARY KEY (motie_id, stemming_id, fractie, kamerlid),
    FOREIGN KEY (motie_id, stemming_id) REFERENCES motie(motie_id, stemming_id)
);

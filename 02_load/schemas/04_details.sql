CREATE TABLE details (
    motie_id TEXT NOT NULL,
    stemming_id TEXT NOT NULL,
    fractie TEXT,
    zetels TEXT,
    kamerlid TEXT,
    stem TEXT,
    niet_deelgenomen TEXT,
    PRIMARY KEY (motie_id, stemming_id, fractie),
    FOREIGN KEY (motie_id, stemming_id) REFERENCES motie(motie_id, stemming_id)
);

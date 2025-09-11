CREATE TABLE motie (
    motie_id TEXT NOT NULL,
    stemming_id TEXT NOT NULL,
    motie_did TEXT,
    document_nr TEXT,
    datum DATE,
    titel TEXT,
    type TEXT,
    text TEXT,
    is_fallback BOOLEAN,
    download TEXT,
    besluit TEXT,
    uitslag TEXT,
    voor INT,
    vereist INT,
    totaal INT,
    PRIMARY KEY (motie_id, stemming_id),
    FOREIGN KEY (stemming_id) REFERENCES stemming(stemming_id)
);

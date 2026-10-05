CREATE TABLE sync_cursors (
    source     text        NOT NULL,
    resource   text        NOT NULL,
    cursor     text        NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, resource)
);

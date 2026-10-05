CREATE TABLE commits (
    source          text        NOT NULL,
    repository      text        NOT NULL,
    sha             text        NOT NULL,
    message         text        NOT NULL,
    author_name     text        NOT NULL,
    author_email    text        NOT NULL,
    author_login    text,
    committer_name  text        NOT NULL,
    committer_email text        NOT NULL,
    committer_login text,
    authored_at     timestamptz NOT NULL,
    committed_at    timestamptz NOT NULL,
    url             text        NOT NULL,
    parent_count    integer     NOT NULL,
    additions       integer,
    deletions       integer,
    ingested_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, repository, sha)
);

CREATE INDEX commits_repository_committed_at_idx
    ON commits (source, repository, committed_at DESC);

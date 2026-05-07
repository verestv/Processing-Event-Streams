CREATE KEYSPACE IF NOT EXISTS wiki_stream
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wiki_stream;

CREATE TABLE IF NOT EXISTS page_creates (
    user_id BIGINT,
    domain TEXT,
    created_at TIMESTAMP,
    page_title TEXT,
    PRIMARY KEY ((domain), created_at, user_id, page_title)
) WITH CLUSTERING ORDER BY (created_at DESC);
CREATE TABLE shippers (
    shipper_id STRING(63) NOT NULL,
    shipper_id_tokens TOKENLIST AS (TOKENIZE_NGRAMS(shipper_id)) HIDDEN,
    revision_id STRING(8),
    create_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    delete_time TIMESTAMP OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY(shipper_id, revision_id);

CREATE SEARCH INDEX shipper_id_tokens_idx ON shippers(shipper_id_tokens);

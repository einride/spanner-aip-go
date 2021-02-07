CREATE TABLE shipments (
    shipper_id STRING(63) NOT NULL,
    shipment_id STRING(63) NOT NULL,
    create_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    update_time TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
    delete_time TIMESTAMP OPTIONS (allow_commit_timestamp=true),
    origin_site_id STRING(63),
    destination_site_id STRING(63),
    pickup_earliest_time TIMESTAMP,
    pickup_latest_time TIMESTAMP,
    delivery_earliest_time TIMESTAMP,
    delivery_latest_time TIMESTAMP,
) PRIMARY KEY(shipper_id, shipment_id),
  INTERLEAVE IN PARENT shippers ON DELETE CASCADE;


DROP SCHEMA IF EXISTS hive.alb_db;

CREATE SCHEMA IF NOT EXISTS hive.alb_db WITH (location = 's3a://sre-warehouse/alblogs/');

USE hive.alb_db;

DROP TABLE IF EXISTS hive.alb_db.logs;

CREATE TABLE IF NOT EXISTS hive.alb_db.logs (
    type VARCHAR,
    time VARCHAR,
    elb_id VARCHAR,
    client_ip_port VARCHAR,
    target_ip_port VARCHAR,
    request_processing_time VARCHAR,
    target_processing_time VARCHAR,
    response_processing_time VARCHAR,
    elb_status_code VARCHAR,
    target_status_code VARCHAR,
    received_bytes VARCHAR,
    sent_bytes VARCHAR,
    request VARCHAR,
    user_agent VARCHAR,
    ssl_cipher VARCHAR,
    ssl_protocol VARCHAR,
    target_group_arn VARCHAR,
    trace_id VARCHAR,
    domain_name VARCHAR,
    chosen_cert_arn VARCHAR,
    matched_rule_priority VARCHAR,
    request_creation_time VARCHAR
)
WITH (
    external_location = 's3a://sre-warehouse/alblogs/',
    format = 'CSV',
    csv_separator = ' '
);


partitioned_by = ARRAY['request_creation_time'],
partition_projection_enabled = true,
partition_projection_location_template = 's3a://sre-warehouse/alblogs/${request_creation_time}'


--- [ORC, PARQUET, AVRO, RCBINARY, RCTEXT, SEQUENCEFILE, JSON, OPENX_JSON, TEXTFILE, CSV, REGEX]

SELECT type,time,elb_id FROM hive.alb_db.logs WHERE type = 'http' limit 5;

CALL system.create_empty_partition('hive','alb_db','logs',ARRAY['type'],ARRAY['http']);

CALL system.sync_partition_metadata('alb_db', 'alb_starpay_logs', mode, 'ADD')

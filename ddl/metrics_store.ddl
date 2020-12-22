CREATE TABLE IF NOT EXISTS rmsi.metrics_store_shard ON CLUSTER raw_cluster (
***

    `timestamp` DateTime, 
    `host` Nullable(String),
    `x_device_info` Nullable(String),
    `event_ts` DateTime,
    `subscriber_id` String,
    `device_platform` String,
    `device_id` String,
    `state` String,
    `content_key` String,
    `asset_id` Nullable(Int32),
    `asset_resource_id` Nullable(Int32),
    `channel_sid` Nullable(Int32),
    `content_type` Nullable(String),
    `event_data` String
) 
ENGINE = ReplicatedMergeTree(
    '/clickhouse/rmsi/tables/{shard}/metrics_store',
    '{replica}') 
PARTITION BY toYYYYMM(toDate(timestamp))
ORDER BY (
***
    `timestamp`,
    `event_ts`,
    `subscriber_id`,
    `state`,
    `content_key`) 
SETTINGS index_granularity = 8192


CREATE TABLE IF NOT EXISTS rmsi.metrics_store ON CLUSTER raw_cluster (
    serviceName String,
    entityType String,
    createTime DateTime64(3),
    lastUpdate DateTime64(3),
    billing String,
    message String,
    errorMessage String,
    status String,
    result String,
    login String,
    networkActionType String,
    networkActionTypeName String,
    identificationType String,
    houseId String,
    nodeId String,
    switchId String,
    elementId String,
    resourceId String,
    network String,
    ports String,
    parameters String,
    operationType String,
    realLogin String,
    impersonatedLogin String,
    errorHistory Array(String),
    orderId String,
    requestId String,
    companyId String,
    office String,
    packageId String,
    agreementNumber String,
    timeSlot String,
    comment String,
    transferAction String,
    accessPanelId String,
    screenshotLink String,
    territories String
) 
ENGINE = Distributed(
    createTime,
    raw_cluster,
    rmsi,
    metrics_store_shard,
    cityHash64(login)
) 


\\\\\\\\\\
CREATE TABLE default.metrics_store
(
...
)
ENGINE = MergeTree()
ORDER BY createTime
SETTINGS index_granularity = 8192
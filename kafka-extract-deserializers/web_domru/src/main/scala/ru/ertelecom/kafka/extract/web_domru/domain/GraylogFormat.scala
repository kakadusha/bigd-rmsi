package ru.ertelecom.kafka.extract.web_domru.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class GraylogFormat(
    log_entry_date: String,
    ts: Long,
    response_status: Int,
    remote_addr: Long,
    body_bytes_sent: Long,
    request_time: Double,
    request: String,
    request_method: String,
    host: String,
    upstream_cache_status: String,
    upstream_addr: String,
    http_referrer: String,
    upstream_response_time: Double,
    upstream_header_time: Double,
    upstream_connect_time: Double,
    request_id: String,
    http_user_agent: String,
    cluster: Option[String],
    x_project: String,
    citydomain: String,
    userId: String,
    p_uid: String,
    request_body: String,
    http_x_forwarded_for: Option[String],
    lb_fqdn: Option[String],
    container_id: Option[String],
    uri: String,
    browser_name: Option[String],
    browser_version: Option[String],
    os_name: Option[String],
    os_version: Option[String],
    device: Option[String]
) extends SerializableMessage {}
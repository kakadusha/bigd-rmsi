package ru.ertelecom.kafka.extract.web_domru_sources.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class WebDomruSource(
    log_entry_date: String,
    ts: Long,
    p_uid: Option[String],
    client_id: Option[String],
    screenresolution: Option[String],
    ua: String,
    ip: Long,
    url: Option[String],
    web_id: Option[String],
    user_id: Option[String],
    ref: Option[String],
    http_ref: Option[String],
    gtm: Option[String],
    citydomain: Option[String],
    browser_name: Option[String],
    browser_version: Option[String],
    os_name: Option[String],
    os_version: Option[String],
    device: Option[String],
    utm_source: String,
    utm_medium: String,
    utm_campaign: Option[String],
    utm_content: Option[String],
    utm_term: Option[String],
    gclid: Option[String],
    yclid: Option[String],
    equila_city_id: Option[String]
) extends SerializableMessage {}

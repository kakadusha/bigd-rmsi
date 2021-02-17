package ru.ertelecom.kafka.extract.web_domru_billing_api.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class WebDomruBillingApi(
    log_entry_date: String,
    ts: Long,
    remote_addr: String,
    http_status: Int,
    http_user_agent: String,
    x_request_id: Option[String],
    request_time: Double,
    response_time: Option[Double],
    upstream_addr: Option[String],
    apistatus: Option[Int],
    platform: Option[String],
    apiname: Option[String],
    api_errorcode: Option[Int],
    agreementnumber: Option[String],
    args: Option[String],
    pkg: String,
    city: String,
    oauthclient: Option[String],
    system: String
) extends SerializableMessage {}
package ru.ertelecom.kafka.extract.wifi_ruckus_syslog.domain

case class RuckusSyslogSource(
    timestamp: String,
    message: String,
    host: String,
    severity: String,
    facility: String,
    syslog_tag: String
    ) {}

object RuckusSyslogSource{
  import play.api.libs.functional.syntax._
  import play.api.libs.json._
  implicit val ruckusSyslogSourceReads: Reads[RuckusSyslogSource] = (
    (JsPath \ "timestamp").read[String] and
    (JsPath \ "message").read[String] and
    (JsPath \ "host").read[String] and
    (JsPath \ "severity").read[String] and
    (JsPath \ "facility").read[String] and
    (JsPath \ "syslog-tag").read[String]
    ) (RuckusSyslogSource.apply _)
}
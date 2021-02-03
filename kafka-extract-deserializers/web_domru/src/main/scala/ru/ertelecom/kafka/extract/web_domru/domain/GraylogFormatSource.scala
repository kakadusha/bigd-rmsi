package ru.ertelecom.kafka.extract.web_domru.domain

case class GraylogFormatSource(
    timestamp: String,
    remote_addr: String,
    body_bytes_sent: Long,
    request_time: Double,
    response_status: Int,
    request: String,
    request_method: String,
    host: String,
    upstream_cache_status: String,
    upstream_addr: String,
    http_referrer: String,
    upstream_response_time: String,
    upstream_header_time: String,
    upstream_connect_time: String,
    request_id: String,
    http_user_agent: String,
    cluster: Option[String],
    x_project: String,
    citydomain: String,
    userId: String,
    uid_set: String,
    uid_got: String,
    request_body: String,
    http_x_forwarded_for: Option[String],
    lb_fqdn: Option[String],
    container_id: Option[String]
    ) {}

object GraylogFormatSource {
  import play.api.libs.functional.syntax._
  import play.api.libs.json._

  val fields1to16: Reads[(String, String, Long, Double, Int, String, String, String, String, String, String, String, String, String, String, String)] = (
    (JsPath \ "timestamp").read[String] and
    (JsPath \ "remote_addr").read[String] and
    (JsPath \ "body_bytes_sent").read[Long] and
    (JsPath \ "request_time").read[Double] and
    (JsPath \ "response_status").read[Int] and
    (JsPath \ "request").read[String] and
    (JsPath \ "request_method").read[String] and
    (JsPath \ "host").read[String] and
    (JsPath \ "upstream_cache_status").read[String] and
    (JsPath \ "upstream_addr").read[String] and
    (JsPath \ "http_referrer").read[String] and
    (JsPath \ "upstream_response_time").read[String] and
    (JsPath \ "upstream_header_time").read[String] and
    (JsPath \ "upstream_connect_time").read[String] and
    (JsPath \ "request_id").read[String] and
    (JsPath \ "http_user_agent").read[String]
  ).tupled

  val fields17to26: Reads[(Option[String], String, String, String, String, String, String, Option[String], Option[String], Option[String])] = (
    (JsPath \ "cluster").readNullable[String] and
    (JsPath \ "x-project").read[String] and
    (JsPath \ "citydomain").read[String] and
    (JsPath \ "userId").read[String] and
    (JsPath \ "uid_set").read[String] and 
    (JsPath \ "uid_got").read[String] and 
    (JsPath \ "request_body").read[String] and
    (JsPath \ "http_x_forwarded_for").readNullable[String] and
    (JsPath \ "lb-fqdn").readNullable[String] and
    (JsPath \ "container_id").readNullable[String]
  ).tupled

  val f: ((String, String, Long, Double, Int, String, String, String, String, String, String, String, String, String, String, String), (Option[String], String, String, String, String, String, String, Option[String], Option[String], Option[String])) => GraylogFormatSource = {
    case ((a, b, c, d, e, f, g, h, i, j ,k, l, m, n, o, p), (a1, b1, c1, d1, e1, f1, g1, h1, i1, j1)) => GraylogFormatSource(a, b, c, d, e, f, g, h, i, j ,k, l, m, n, o, p, a1, b1, c1, d1, e1, f1, g1, h1, i1, j1)
  }

  implicit val hugeCaseClassReads: Reads[GraylogFormatSource] = (
    fields1to16 and fields17to26
  ).apply({ f })
}
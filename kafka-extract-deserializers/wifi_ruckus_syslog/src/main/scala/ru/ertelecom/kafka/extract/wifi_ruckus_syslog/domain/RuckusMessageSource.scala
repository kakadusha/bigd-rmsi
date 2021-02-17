package ru.ertelecom.kafka.extract.wifi_ruckus_syslog.domain

case class RuckusMessageSource(
    apMac: String,
    clientMac: String,
    ssid: String,
    apName: String,
    toRadio: Option[String],
    fromApMac: Option[String],
    clientIP: Option[String],
    userName: Option[String],
    vlanId: String,
    radio: String,
    osType: Option[String],
    hostname: Option[String],
    firstAuth: Option[String],
    associationTime: Option[String],
    ipAssignTime: Option[String],
    disconnectTime: Option[String],
    sessionDuration: Option[String],
    disconnectReason: Option[String],
    rxBytes: Option[String],
    txBytes: Option[String],
    peakRx: Option[String],
    peakTx: Option[String],
    rssi: Option[String],
    receivedSignalStrength: Option[String],
    zoneName: String,
    apLocation: Option[String],
    apGps: Option[String],
    apIpAddress: String,
    apDescription: Option[String]
    ) {}

object RuckusMessageSource {
  import play.api.libs.functional.syntax._
  import play.api.libs.json._

  val fields1to16: Reads[(String, String, String, String, Option[String], Option[String], Option[String], Option[String], String, String, Option[String], Option[String], Option[String], Option[String], Option[String], Option[String])] = (
    (JsPath \ "apMac").read[String] and
    (JsPath \ "clientMac").read[String] and
    (JsPath \ "ssid").read[String] and
    (JsPath \ "apName").read[String] and
    (JsPath \ "toRadio").readNullable[String] and
    (JsPath \ "fromApMac").readNullable[String] and
    (JsPath \ "clientIP").readNullable[String] and
    (JsPath \ "userName").readNullable[String] and
    (JsPath \ "vlanId").read[String] and
    (JsPath \ "radio").read[String] and
    (JsPath \ "osType").readNullable[String] and
    (JsPath \ "hostname").readNullable[String] and
    (JsPath \ "firstAuth").readNullable[String] and
    (JsPath \ "associationTime").readNullable[String] and
    (JsPath \ "ipAssignTime").readNullable[String] and
    (JsPath \ "disconnectTime").readNullable[String]
  ).tupled

  val fields17to29: Reads[(Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], String, Option[String], Option[String], String, Option[String])] = (
    (JsPath \ "sessionDuration").readNullable[String] and
    (JsPath \ "disconnectReason").readNullable[String] and
    (JsPath \ "rxBytes").readNullable[String] and
    (JsPath \ "txBytes").readNullable[String] and
    (JsPath \ "peakRx").readNullable[String] and 
    (JsPath \ "peakTx").readNullable[String] and 
    (JsPath \ "rssi").readNullable[String] and
    (JsPath \ "receivedSignalStrength").readNullable[String] and
    (JsPath \ "zoneName").read[String] and
    (JsPath \ "apLocation").readNullable[String] and
    (JsPath \ "apGps").readNullable[String] and
    (JsPath \ "apIpAddress").read[String] and
    (JsPath \ "apDescription").readNullable[String]
  ).tupled

  val f: ((String, String, String, String, Option[String], Option[String], Option[String], Option[String], String, String, Option[String], Option[String], Option[String], Option[String], Option[String], Option[String]), (Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], Option[String], String, Option[String], Option[String], String, Option[String])) => RuckusMessageSource = {
    case ((a, b, c, d, e, f, g, h, i, j ,k, l, m, n, o, p), (a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1)) => RuckusMessageSource(a, b, c, d, e, f, g, h, i, j ,k, l, m, n, o, p, a1, b1, c1, d1, e1, f1, g1, h1, i1, j1, k1, l1, m1)
  }

  implicit val hugeCaseClassReads: Reads[RuckusMessageSource] = (
    fields1to16 and fields17to29
  ).apply({ f })
}
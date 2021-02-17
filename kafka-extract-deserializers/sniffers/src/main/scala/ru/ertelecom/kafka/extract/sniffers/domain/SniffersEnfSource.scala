package ru.ertelecom.kafka.extract.sniffers.domain

case class SniffersEnfSource(
                              snif_id: String,
                              snif_ip: String,
                              mac: String,
                              channel: String,
                              vendor: String,
                              known_from: String,
                              last_seen: String,
                              rssi_max: String,
                              rssi: String,
                              notified_count: String,
                              frames_count: String

                            )
{}
object SniffersEnfSource{
  import play.api.libs.functional.syntax._
  import play.api.libs.json._
  implicit val sniffersEnfSourceReads: Reads[SniffersEnfSource] = (
    (JsPath \ "SnifID").read[String] and
    (JsPath \ "SnifIP").read[String] and
      (JsPath \ "MAC").read[String] and
      (JsPath \ "Channel").read[String] and
      (JsPath \ "Vendor").read[String] and
      (JsPath \ "KnownFrom").read[String] and
      (JsPath \ "LastSeen").read[String] and
      (JsPath \ "RSSImax").read[String] and
      (JsPath \ "RSSI").read[String] and
      (JsPath \ "NotifiedCount").read[String] and
      (JsPath \ "FramesCount").read[String]
    ) (SniffersEnfSource.apply _)
}


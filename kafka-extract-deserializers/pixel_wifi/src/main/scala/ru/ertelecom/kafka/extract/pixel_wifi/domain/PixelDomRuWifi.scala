package ru.ertelecom.kafka.extract.pixel_wifi.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class PixelDomRuWifi(
                           loginid: Option[String],
                           acctid: Option[String],
                           deviceip: Option[String],
                           devicemac: Option[String],
                           locationid: Option[String],
                           pageid: String,
                           routerid: Option[String],
                           clientts: Long,
                           datehourminute: Long,
                           client_id: Option[String],
                           ua: String,
                           ip: String,
                           puid: Option[String],
                           http_ref: Option[String],
                           citydomain: Option[String],
                           userid: Option[String],
                           vkid: Option[String],
                           okid: Option[String],
                           fbid: Option[String],
                           twid: Option[String],
                           instid: Option[String],
                           cityid: Option[String],
                           browser_name: Option[String],
                           browser_version: Option[String],
                           os_name: Option[String],
                           os_version: Option[String],
                           device: Option[String],
                           language: Option[String],
                           year: Int,
                           month: Int
                      ) extends SerializableMessage {
 }

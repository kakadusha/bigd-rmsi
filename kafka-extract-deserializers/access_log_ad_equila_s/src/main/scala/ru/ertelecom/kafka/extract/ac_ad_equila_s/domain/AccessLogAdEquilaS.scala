package ru.ertelecom.kafka.extract.ac_ad_equila_s.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class AccessLogAdEquilaS(
                               ts: Long,
                               ip: Long,
                               campaign_id: Option[Long],
                               machine: Option[String],
                               sid: Option[String],
                               ua: String,
                               uid: Option[String],
                               puid: Option[String],
                               ref: Option[String],
                               city_id: Option[String],
                               user_id: Option[String],
                               ourl: Option[String],
                               http_location: Option[String],
                               login: Option[String],
                               for_day: String
                             ) extends SerializableMessage

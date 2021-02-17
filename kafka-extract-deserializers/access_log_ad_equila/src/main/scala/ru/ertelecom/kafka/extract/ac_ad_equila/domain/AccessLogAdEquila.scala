package ru.ertelecom.kafka.extract.ac_ad_equila.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class AccessLogAdEquila (
                             action: String,
                             ts: Long,
                             ip: Long,
                             req_uri: String,
                             ua: String,
                             uid: Option[String],
                             puid: Option[String],
                             ref: Option[String],
                             city_id: Option[String],
                             user_id: Option[String],
                             billing_id: Option[Int],
                             campaign_id: Option[Long],
                             login: Option[String],
                             for_day: String
                             ) extends SerializableMessage

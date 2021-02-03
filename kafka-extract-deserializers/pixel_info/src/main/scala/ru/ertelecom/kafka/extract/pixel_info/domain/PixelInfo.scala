package ru.ertelecom.kafka.extract.pixel_info.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class PixelInfo(
                      datetime: Long,
                      ip: String,
                      pixel_endpoint: String,
                      user_agent: String,
                      uid: Option[String],
                      p_uid: String,
                      campid: Option[Int],
                      machine: Option[String],
                      ourl: Option[String],
                      u: Option[String],
                      timestamp_http_ref: Option[Long],
                      sid: Option[String],
                      city: Option[String],
                      year: Int,
                      month: Int
                    ) extends SerializableMessage {}

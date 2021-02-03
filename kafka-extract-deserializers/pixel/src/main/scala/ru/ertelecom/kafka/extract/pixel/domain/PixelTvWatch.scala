package ru.ertelecom.kafka.extract.pixel.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class PixelTvWatch(
                         message_timestamp: java.sql.Timestamp,
                         ip: String,
                         event_timestamp: java.sql.Timestamp,
                         event_name: String,
                         subscriber: String,
                         platform: String,
                         device: String,
                         device_id: String,
                         core_version: Option[String],
                         web_app_version: Option[String],
                         asset_id: Option[Int],
                         season: Option[Int],
                         episode: Option[Int],
                         lcn: Option[Int],
                         channel_sid: Option[Int],
                         real_event_start: java.sql.Timestamp,
                         catch_up_start: Option[java.sql.Timestamp],
                         real_event_finish: java.sql.Timestamp,
                         catch_up_finish: Option[java.sql.Timestamp],
                         duration: Int,
                         user_agent: String
                       ) extends SerializableMessage{

}

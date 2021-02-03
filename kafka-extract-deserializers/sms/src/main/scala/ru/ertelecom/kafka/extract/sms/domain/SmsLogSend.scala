package ru.ertelecom.kafka.extract.sms.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class SmsLogSend(
                     created_at: Long,
                     loaded_at: Long,
                     sms_query_id: Long,
                     phone: String,
                     msg_ru: String,
                     msg_en: String,
                     msg_zh: String,
                     batch_id: Long,
                     emulate: Option[Int],
                     city_id: Option[Int],
                     billing_id: Option[Int],
                     `type`: Option[Int]
                     )
  extends SerializableMessage{}

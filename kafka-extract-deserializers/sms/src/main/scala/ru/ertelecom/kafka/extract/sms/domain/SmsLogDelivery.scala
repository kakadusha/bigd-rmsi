package ru.ertelecom.kafka.extract.sms.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class SmsLogDelivery(
                                query_ts: Long,
                                loaded_at: Long,
                                sms_query_id: Long,
                                phone: String,
                                status: Short,
                                batch_id: Long,
                                emulate: Option[Int],
                                city_id: Option[Int],
                                billing_id: Option[Int],
                                `type`: Option[Int]
                               ) extends SerializableMessage{}

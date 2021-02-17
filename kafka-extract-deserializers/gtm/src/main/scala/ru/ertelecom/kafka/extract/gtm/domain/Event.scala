package ru.ertelecom.kafka.extract.gtm.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class Event(
    event_name: String,
    event_ts: Long,
    tag_id: String,
    tag_status: String,
    tag_estime: Long,
    ref: String
) extends SerializableMessage {}
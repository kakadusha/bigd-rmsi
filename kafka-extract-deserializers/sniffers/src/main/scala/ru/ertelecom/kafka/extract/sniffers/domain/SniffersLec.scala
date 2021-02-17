package ru.ertelecom.kafka.extract.sniffers.domain

import org.joda.time.DateTime
import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class SniffersLec(
                        ts: Long,
                        sensor: String,
                        mac: String,
                        signal: Int,
                        channel: Int,
                        ip: Long
                      )extends SerializableMessage {

}


package ru.ertelecom.kafka.extract.sniffers.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class SniffersEnf(
                        sensor: String,
                        snif_ip: Long,
                        mac: String,
                        channel: Int,
                        vendor: String,
                        known_from: Int,
                        ts: Long,
                        rssi_max: Int,
                        rssi: Int,
                        notified_count: Int,
                        frames_count: Int
)extends SerializableMessage {

}

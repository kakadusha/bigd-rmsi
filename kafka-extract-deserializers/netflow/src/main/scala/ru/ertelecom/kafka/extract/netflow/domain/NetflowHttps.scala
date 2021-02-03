package ru.ertelecom.kafka.extract.netflow.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class NetflowHttps(
                       bid: Short,
                       ts: Long,
                       wip: Long,
                       cid: Option[String],
                       sni: String,
                       cip: String,
                       forDay: String
                       ) extends SerializableMessage {}

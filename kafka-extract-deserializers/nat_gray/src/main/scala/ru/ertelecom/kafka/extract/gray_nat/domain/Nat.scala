package ru.ertelecom.kafka.extract.gray_nat.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage

case class Nat(
                billingId: Short,
                login: Option[String],
                `type`: Short,
                grayIp: Long,
                whiteIp: Long,
                startPort: Int,
                endPort: Int
              ) extends SerializableMessage {}

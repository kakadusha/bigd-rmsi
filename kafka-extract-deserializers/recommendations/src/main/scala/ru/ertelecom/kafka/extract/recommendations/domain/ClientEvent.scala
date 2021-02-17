package ru.ertelecom.kafka.extract.recommendations.domain

import ru.ertelecom.kafka.extract.core.serde.SerializableMessage


case class ClientEvent(
                        timestam: String,
                        subscriberId: Long,
                        log_id: String,
                        itemType: Option[String],
                        platformId: Int,
                        deviceId: Long,
                        eventType: Int,
                        ts: Option[Long],
                        assetId: Option[Long],
                        useCaseId: Option[String],
                        extIdCity: String,
                        scheduleProgramId: Option[Long],
                        assetPosition: Option[Int],
                        recommendationId: Option[String],
                        changeType: Option[String],
                        isCatchup: Option[Boolean],
                        isCatchupStartover: Option[Boolean],
                        channelSid: Option[Int],
                        position: Option[Int],
                        lcn: Option[Int],
                        started: Option[Long],
                        finish: Option[Long],
                        transactionId: Option[Long],
                        progressValue: Option[Long],
                        segmentId: Option[Int],
                        groupId: Int,
                        year: Int,
                        month: Int,
                        for_day: String
                      ) extends SerializableMessage {

}

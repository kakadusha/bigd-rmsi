package ru.ertelecom.kafka.extract.recommendations.domain

import play.api.libs.json.Json


case class ClientEventItem(
                            `type`: String,
                            ts: Option[Long],
                            assetId: Option[Long],
                            scheduleProgramId: Option[Long],
                            itemType: Option[String],
                            usecaseId: Option[String],
                            assetPosition: Option[Int],
                            recommendationId: Option[String],
                            changeType: Option[String],
                            isCatchup: Option[Boolean],
                            isCatchupStartover: Option[Boolean],
                            channelSid: Option[Int],
                            position: Option[Int],
                            lcn: Option[Int],
                            start: Option[Long],
                            finish: Option[Long],
                            transactionId: Option[Long],
                            progressValue: Option[Long]
                          ) {

}

object ClientEventItem {
  implicit val eventItemFormat = Json.format[ClientEventItem]
}

package ru.ertelecom.kafka.extract.recommendations.domain

import play.api.libs.json.{JsPath, Json, Reads}


case class ClientEventSource(
                              id: String,
                              subscriberId: Long,
                              extIdCity: String,
                              groupId: Int,
                              platformId: Int,
                              deviceId: Long,
                              segmentId: Option[Int],
                              timeStamp: Long,
                              item: ClientEventItem
                            ) {

}

object ClientEventSource {

  import play.api.libs.functional.syntax._
  import play.api.libs.json._

  implicit val clientEventSourceReads: Reads[ClientEventSource] = (
    (JsPath \ "id").read[String] and
      (JsPath \ "subscriberId").read[Long] and
      (JsPath \ "extIdCity").read[String] and
      (JsPath \ "groupId").read[Int] and
      (JsPath \ "platformId").read[Int] and
      (JsPath \ "deviceId").read[Long] and
      (JsPath \ "segmentId").readNullable[Int] and
      (JsPath \ "timeStamp").read[Long] and
      (JsPath \ "item").read[ClientEventItem]
    ) (ClientEventSource.apply _)
}
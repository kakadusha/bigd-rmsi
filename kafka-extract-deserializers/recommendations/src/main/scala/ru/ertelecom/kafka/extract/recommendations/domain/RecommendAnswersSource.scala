package ru.ertelecom.kafka.extract.recommendations.domain

case class RecommendAnswersSource(
                                 id: String,
                                 subscriberId: Long,
                                 extIdCity: String,
                                 platformId: Int,
                                 deviceId: Long,
                                 segmentId: Option[Int],
                                 timeStamp: Long,
                                 showcaseItemType: String,
                                 showcaseName: String,
                                 recommendationId: Int,
                                 externalSortIds: Option[Array[Int]],
                                 externalSortFields: Option[Array[String]],
                                 recCount: Option[Int]
                                 ) {

}

object RecommendAnswersSource {
  import play.api.libs.functional.syntax._
  import play.api.libs.json._

  implicit val recommendAnswersSourceReads: Reads[RecommendAnswersSource] = (
    (JsPath \ "id").read[String] and
      (JsPath \ "subscriberId").read[Long] and
      (JsPath \ "extIdCity").read[String] and
      (JsPath \ "platformId").read[Int] and
      (JsPath \ "deviceId").read[Long] and
      (JsPath \ "segmentId").readNullable[Int] and
      (JsPath \ "timeStamp").read[Long] and
      (JsPath \ "showcaseItemType").read[String] and
      (JsPath \ "showcaseName").read[String] and
      (JsPath \ "recommendationId").read[Int] and
      (JsPath \ "externalSortIds").readNullable[Array[Int]] and
      (JsPath \ "externalSortFields").readNullable[Array[String]] and
      (JsPath \ "recCount").readNullable[Int]
  ) (RecommendAnswersSource.apply _)
}

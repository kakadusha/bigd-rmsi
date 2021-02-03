package ru.ertelecom.kafka.extract.recommendations.domain

case class RecommendationAnswers(
                                  timestam: String,
                                  answerRecommendationId: Option[Int],
                                  subscriberId: Long,
                                  showcaseItemType: String,
                                  platformId: Int,
                                  answerRecommendationIdIndex: Option[Int],
                                  uuid: String,
                                  deviceId: Long,
                                  extIdCity: String,
                                  segmentId: Option[Int],
                                  showcaseName: String,
                                  recommendationId: Int,
                                  externalSortField: Option[String],
                                  recCount: Option[Int],
                                  year: Int,
                                  month: Int
                                ) {

}

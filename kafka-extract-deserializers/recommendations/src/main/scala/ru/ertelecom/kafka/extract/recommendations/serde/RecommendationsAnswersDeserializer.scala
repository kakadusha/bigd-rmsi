package ru.ertelecom.kafka.extract.recommendations.serde

import java.sql.Timestamp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import play.api.libs.json.Json
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.recommendations.domain.{RecommendAnswersSource, RecommendationAnswers}

class RecommendationsAnswersDeserializer(appConf: Config) extends Deserializer(appConf){

  lazy val isoDateTimeFmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  override def deserialize(): UserDefinedFunction = udf {payload: Array[Byte] =>
    this.parseJson(payload)
  }

  def parseJson(payload: Array[Byte]): Option[Array[RecommendationAnswers]] = {
    try {
      val answersSource = Json.parse(new String(payload)).as[RecommendAnswersSource]
      val datetime = new DateTime(answersSource.timeStamp * 1000L, DateTimeZone.UTC)
      var result: Array[RecommendationAnswers] = Array()
      if (answersSource.externalSortIds.isDefined && answersSource.externalSortIds.get.length > 0) {
        val sortIds = answersSource.externalSortIds.get
        val sortFields = answersSource.externalSortFields.get
        for (i <- sortIds.indices) {
          result :+= RecommendationAnswers(
            timestam = isoDateTimeFmt.print(datetime),
            answerRecommendationId = Option.apply(sortIds(i)),
            subscriberId = answersSource.subscriberId,
            showcaseItemType = answersSource.showcaseItemType,
            platformId = answersSource.platformId,
            answerRecommendationIdIndex = Option.apply(i),
            uuid = answersSource.id,
            deviceId = answersSource.deviceId,
            extIdCity = answersSource.extIdCity,
            segmentId = answersSource.segmentId,
            showcaseName = answersSource.showcaseName,
            recommendationId = answersSource.recommendationId,
            externalSortField = Option.apply(sortFields(i)),
            recCount = answersSource.recCount,
            year = datetime.getYear,
            month = datetime.getMonthOfYear
          )
        }

      } else {
        result :+= RecommendationAnswers(
          timestam = isoDateTimeFmt.print(datetime),
          answerRecommendationId = Option.empty,
          subscriberId = answersSource.subscriberId,
          showcaseItemType = answersSource.showcaseItemType,
          platformId = answersSource.platformId,
          answerRecommendationIdIndex = Option.empty,
          uuid = answersSource.id,
          deviceId = answersSource.deviceId,
          extIdCity = answersSource.extIdCity,
          segmentId = answersSource.segmentId,
          showcaseName = answersSource.showcaseName,
          recommendationId = answersSource.recommendationId,
          externalSortField = Option.empty[String],
          recCount = answersSource.recCount,
          year = datetime.getYear,
          month = datetime.getMonthOfYear
        )
      }
      Option.apply(result)
    } catch {
      case e: Exception => Option.empty
    }
  }
}

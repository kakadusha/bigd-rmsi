package ru.ertelecom.kafka.extract.recommendations.serde

import java.sql.Timestamp

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.Json
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.recommendations.domain.{ClientEvent, ClientEventSource}

class ClientEventsDeserializer(appConfig: Config) extends Deserializer(appConfig) with Logging {

  lazy val isoDateTimeFmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  lazy val forDayFmt = DateTimeFormat.forPattern("yyyy-MM-dd")

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    this.parseJson(payload)
  }

  def parseJson(payload: Array[Byte]): Option[ClientEvent] = {
    try {
      val clientEventSource = Json.parse(new String(payload)).as[ClientEventSource]
      val datetime = new DateTime(clientEventSource.timeStamp * 1000L, DateTimeZone.UTC)
      val nestedItem = clientEventSource.item
      val recId = if (nestedItem.recommendationId.isDefined && nestedItem.recommendationId.get == "null") {
        Option.empty
      } else {
        nestedItem.recommendationId
      }
      val changeType = if (nestedItem.changeType.isDefined && nestedItem.changeType.get == "null") {
        Option.empty
      } else {
        nestedItem.changeType
      }
      Option.apply(ClientEvent(
        timestam = isoDateTimeFmt.print(datetime),
        subscriberId = clientEventSource.subscriberId,
        log_id = clientEventSource.id,
        itemType = nestedItem.itemType,
        platformId = clientEventSource.platformId,
        deviceId = clientEventSource.deviceId,
        eventType = nestedItem.`type`.toInt,
        ts = nestedItem.ts,
        assetId = nestedItem.assetId,
        useCaseId = nestedItem.usecaseId,
        extIdCity = clientEventSource.extIdCity,
        scheduleProgramId = nestedItem.scheduleProgramId,
        assetPosition = nestedItem.assetPosition,
        recommendationId = recId,
        changeType = changeType,
        isCatchup = nestedItem.isCatchup,
        isCatchupStartover = nestedItem.isCatchupStartover,
        channelSid = nestedItem.channelSid,
        position = nestedItem.position,
        lcn = nestedItem.lcn,
        started = nestedItem.start,
        finish = nestedItem.finish,
        transactionId = nestedItem.transactionId,
        progressValue = nestedItem.progressValue,
        segmentId = clientEventSource.segmentId,
        groupId = clientEventSource.groupId,
        year = datetime.getYear,
        month = datetime.getMonthOfYear,
        for_day = forDayFmt.print(datetime)
      ))
    } catch {
      case e: Exception => Option.empty[ClientEvent]
    }
  }
}

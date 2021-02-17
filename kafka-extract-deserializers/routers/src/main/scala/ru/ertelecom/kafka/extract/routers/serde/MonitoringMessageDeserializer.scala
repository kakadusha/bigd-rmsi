package ru.ertelecom.kafka.extract.routers.serde

import java.time.{LocalDate, LocalDateTime, OffsetDateTime, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import proto.message.MonitoringMessage
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.routers.domain.MonitoringMessageTransformed

class MonitoringMessageDeserializer(appConf: Config) extends Deserializer(appConf) {
  private lazy val RFC_3339_NANO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.nxxx"
  private lazy val rfcFormatter = DateTimeFormatter.ofPattern(RFC_3339_NANO_FORMAT)

  private val noValue: MonitoringMessageTransformed = null

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    try {
      val message = MonitoringMessage.parseFrom(payload)
      convertMonMessage(message)
    } catch {
      case e: Exception => Array[MonitoringMessageTransformed](noValue)
    }
  }

  def convertMonMessage(msg: MonitoringMessage): Array[MonitoringMessageTransformed] = {
    msg.data.map(d => {
      val zonedTs = parseTimestampToZoned(msg.monitoringTimestamp)
      MonitoringMessageTransformed(
        monitoringDate = parseDate(msg.monitoringDate),
        monitoringTimestamp = zonedTs.toEpochSecond * 1000,
        deviceId = msg.deviceId,
        hostmodelId = msg.hostmodelId,
        terminalResource = msg.terminalResource,
        methodName = msg.methodName,
        city = msg.city,
        monitoringTaskId = msg.monitoringTaskId,
        mask = d.mask,
        branch = d.branch,
        metricValue = d.value,
        for_day = zonedTs.format(DateTimeFormatter.ISO_LOCAL_DATE)
      )
    }).toArray
  }

  private def parseDate(date: String): String = {
    if (date.length == 10) {
      date
    } else {
      LocalDateTime.parse(date, rfcFormatter).format(DateTimeFormatter.ISO_DATE)
    }
  }

  private def parseTimestampToZoned(tsString: String): OffsetDateTime = {
    if (tsString.length == 25) {
      OffsetDateTime.parse(tsString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
    } else {
      OffsetDateTime.parse(tsString, rfcFormatter)
    }
  }
}

package ru.ertelecom.kafka.extract.routers.serde

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Calendar

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import proto.message.MonitoringWiFiMessage
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.routers.domain.MonitoringWifiMessageTransformed

class MonitoringWifiMessageDeserializer(appConf: Config) extends Deserializer(appConf) {
  private lazy val RFC_3339_NANO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.nxxx"
  private lazy val rfcFormatter = DateTimeFormatter.ofPattern(RFC_3339_NANO_FORMAT)

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    try {
      val message = MonitoringWiFiMessage.parseFrom(payload)
      val monDate = parseDate(message.monitoringDate)
      val monDateCal = Calendar.getInstance()
      monDateCal.setTime(monDate)
      Option.apply(MonitoringWifiMessageTransformed(
        monDate,
        parseTimestamp(message.monitoringTimestamp).getTime,
        message.deviceId,
        message.hostmodelId,
        message.terminalResource,
        message.methodName,
        message.city,
        message.monitoringTaskId,
        message.data,
        monDateCal.get(Calendar.YEAR),
        monDateCal.get(Calendar.MONTH) + 1,
        monDateCal.get(Calendar.DAY_OF_MONTH)
      ))
    } catch {
      case e: Exception => Option.empty[MonitoringWifiMessageTransformed]
    }
  }

  private def parseDate(date: String): java.sql.Date = {
    if (date.length == 10)
      java.sql.Date.valueOf(LocalDate.parse(date, DateTimeFormatter.ISO_DATE))
    else
      java.sql.Date.valueOf(LocalDate.parse(date, rfcFormatter))
  }

  private def parseTimestamp(tsString: String): java.sql.Timestamp = {
    if (tsString.length == 25)
      java.sql.Timestamp.valueOf(LocalDateTime.parse(tsString, DateTimeFormatter.ISO_OFFSET_DATE_TIME))
    else
      java.sql.Timestamp.valueOf(LocalDateTime.parse(tsString, rfcFormatter))
  }
}

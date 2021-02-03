package ru.ertelecom.dpi.eql.serde

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import ru.ertelecom.dpi.eql.domain.EquilaDpiCampaignLog
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.udf.funcs.ip.IpTransform

import scala.collection.immutable.Stream.Empty

class EquilaDpiCampaignDeserializer(appConf: Config) extends Deserializer(appConf) {

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    parseTsv(new String(payload))
  }

  def parseTsv(tsv: String):Option[EquilaDpiCampaignLog] = {
    try {
      val fields = tsv
        .replaceAll("\n", "\t")
        .replaceAll("(\t+)", "\t")
        .replaceAll("(\t)$", "")
        .split('\t')

      val tsMillis = fields(2).toLong * 1000L
      val dt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(tsMillis), ZoneId.of("UTC"))

      var referer: String = null
      if (fields.length >= 13) {
        if (fields(12) == "-") /* "referer" attribute */ {
          referer = null
        } else {
          referer = fields(12)
        }
      }

      var msgType: Option[Int] = null
      if (fields.length >= 14) {
        msgType = Option(fields(13).toInt)
      } else {
        msgType = if (fields(10).toInt == 0) /* fields(10) - event_type */ Some(0) else Some(100)
      }


      Option.apply(
        EquilaDpiCampaignLog(
          billing_id = fields(0).toInt,
          box_id = fields(1).toInt,
          ts = tsMillis,
          login = fields(3),
          device_id = fields(4),
          client_private_ip = IpTransform.ip4ToLong(fields(5)),
          client_request_ip = IpTransform.ip4ToLong(fields(6)),
          url = fields(7),
          ua = fields(8),
          campaign_id = fields(9).toLong,
          event_type = fields(10).toInt,
          sid = fields(11),
          referer = referer,
          msgType = msgType,
          for_day = dt.format(DateTimeFormatter.ISO_LOCAL_DATE)
        )
      )
    } catch {
      case _: Exception => Option.empty[EquilaDpiCampaignLog]
    }
  }
}

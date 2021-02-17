package ru.ertelecom.kafka.extract.sniffers.serde

import java.nio.charset.StandardCharsets

import scala.util.matching.Regex
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.{JsValue, Json}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.sniffers.domain.{SniffersEnf, SniffersEnfSource}
import ru.ertelecom.kafka.extract.sniffers.udf.IpToLong


class SniffersEnfDeserializer(appConfig: Config) extends Deserializer(appConfig) with Logging {
  lazy val isoDateTimeFmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    this.parseJson(payload)
  }


  def reformat_mac(raw_mac: String): StringBuilder = {
    val mac = new StringBuilder()
    val keyValPattern = new Regex("^([a-zA-Z0-9]{2})[:.]?([a-zA-Z0-9]{2})[:.]?([a-zA-Z0-9]{2})[:.]?([a-zA-Z0-9]{2})[:.]?([a-zA-Z0-9]{2})[:.]?([a-zA-Z0-9]{2})$")
    keyValPattern.findAllIn(raw_mac.toUpperCase()).matchData foreach {
      m => mac.append(m.group(1) + ":" + m.group(2) + ":" + m.group(3) + ":" + m.group(4) + ":" + m.group(5) + ":" + m.group(6))
    }
    return mac
  }

  def parseJson(payload: Array[Byte]): Option[Array[SniffersEnf]] = {
    try {
      var result: Array[SniffersEnf] = Array()
      val sniffersEnfSource: List[SniffersEnfSource] = Json.parse(new String(payload)).as[List[SniffersEnfSource]]
      for (el <- sniffersEnfSource) {
        val ts = el.last_seen.toLong
        val new_ip = new IpToLong()
        result :+= SniffersEnf(
          sensor = el.snif_id.toUpperCase(),
          snif_ip = new_ip.ipToLong(el.snif_ip),
          mac = el.mac.toUpperCase().replaceAll(":", ""),
          channel = el.channel.toInt,
          vendor = el.vendor,
          known_from = el.known_from.toInt,
          ts = ts,
          rssi_max = el.rssi_max.toInt,
          rssi = el.rssi.toInt,
          notified_count = el.notified_count.toInt,
          frames_count = el.frames_count.toInt
        )
      }
      Option.apply(result)
    }
    catch {
      case e: Exception => Option.empty
    }
  }
}
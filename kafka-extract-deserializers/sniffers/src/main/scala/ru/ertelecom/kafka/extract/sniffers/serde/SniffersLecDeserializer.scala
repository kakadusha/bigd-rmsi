package ru.ertelecom.kafka.extract.sniffers.serde

import java.nio.charset.StandardCharsets

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import play.api.libs.json.Json
import ru.ertelecom.kafka.extract.sniffers.domain.{IntNastedArrayValue, SniffersLec, SniffersLecSource, StringNastedArrayValue}
import ru.ertelecom.kafka.extract.sniffers.udf.IpToLong

class SniffersLecDeserializer(appConfig: Config) extends Deserializer(appConfig) with Logging {
  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    this.parseJson(payload)
  }
  lazy val isoDateTimeFmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  def parseJson(payload: Array[Byte]): Option[Array[SniffersLec]] = {
    try {
      val tsv = new String(payload, StandardCharsets.UTF_8).split("\t", -1)
      val sniffersLecSource = Json.parse(new String(tsv(3))).as[SniffersLecSource]
      val ts = tsv(0)
      val ip = tsv(1)

      val sensor_mac = sniffersLecSource.sensor.toUpperCase().replaceAll(":", "")
      val mac_array = sniffersLecSource.col.get

      val result = mac_array.map(el => {
        val pre_sec = el(3).asInstanceOf[StringNastedArrayValue].s.split("\\.")
        val sec = pre_sec(0).toLong
        val datetime = new DateTime(ts, DateTimeZone.UTC)
        val new_ip = new IpToLong()
        SniffersLec(
          ts = sec,
          sensor = sensor_mac,
          mac = el(0).asInstanceOf[StringNastedArrayValue].s.replaceAll(":", ""),
          signal = el(1).asInstanceOf[IntNastedArrayValue].i,
          channel = el(2).asInstanceOf[IntNastedArrayValue].i,
          ip = new_ip.ipToLong(ip)
        )
      })
      Option.apply(result)
    } catch {
      case e: Exception => Option.empty
    }
  }
}

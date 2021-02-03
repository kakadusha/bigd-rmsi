package ru.ertelecom.kafka.extract.marker.serde

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.marker.domain.MarkerParsed

class MarkerDeserializer (appConf: Config) extends Deserializer(appConf){
  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    this.parseTsv(payload)
  }

  def parseTsv(bytes: Array[Byte]): Option[MarkerParsed] = {
    val fields = new String(bytes).split("\t")
    if(fields.length != 4) {
      Option.empty
    } else {
      val ts = fields(2).toLong
      val dt = ZonedDateTime.ofInstant(Instant.ofEpochSecond(ts), ZoneId.of("UTC"))
      Some(MarkerParsed(
        triggered_date = dt.format(DateTimeFormatter.ISO_LOCAL_DATE),
        triggered_ts = ts,
        billing_id = fields(0).toInt,
        login = fields(1),
        marker = fields(3)
      ))
    }
  }
}

package ru.ertelecom.kafka.extract.pixel.serde

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.pixel.domain.PixelTvWatch

class PixelTvWatchDeserializer(appConf: Config) extends Deserializer(appConf) {
  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    try {
      val tsv = new String(payload, StandardCharsets.UTF_8).split("\t", -1)
      val queryParams = tsv(2).split("\\?", 2)
      val params = queryParams(1).split("&").map(v => {
        val m = v.split("=", 2).map(s => URLDecoder.decode(s, "UTF-8"))
        m(0) -> m(1)
      }).filter(_._2 != "null").toMap
      Option.apply(PixelTvWatch(
        message_timestamp = java.sql.Timestamp.valueOf(LocalDateTime.parse(tsv(0), DateTimeFormatter.ISO_OFFSET_DATE_TIME)),
        ip = tsv(1),
        event_timestamp = java.sql.Timestamp.from(Instant.ofEpochSecond(params("ts").toLong)),
        event_name = params("en"),
        subscriber = params("uid"),
        platform = params("pt"),
        device = params("dvt"),
        device_id = params("did"),
        core_version = params.get("cv"),
        web_app_version = params.get("wa"),
        asset_id = params.get("itid").map(_.toInt),
        season = params.get("ss").map(_.toInt),
        episode = params.get("ep").map(_.toInt),
        lcn = params.get("lcn").map(_.toInt),
        channel_sid = params.get("cs").map(_.toInt),
        real_event_start = java.sql.Timestamp.from(Instant.ofEpochSecond(params("res").toLong)),
        catch_up_start = params.get("ces").map((ces: String) => java.sql.Timestamp.from(Instant.ofEpochSecond(ces.toLong))),
        real_event_finish = java.sql.Timestamp.from(Instant.ofEpochSecond(params("ref").toLong)),
        catch_up_finish = params.get("cef").map((cef: String) => java.sql.Timestamp.from(Instant.ofEpochSecond(cef.toLong))),
        duration = params("ed").toInt,
        user_agent = tsv(3)
      ))
    } catch {
      case e: Exception => Option.empty[PixelTvWatch]
    }
  }
}

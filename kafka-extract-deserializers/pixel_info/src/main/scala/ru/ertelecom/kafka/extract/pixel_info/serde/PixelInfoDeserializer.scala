package ru.ertelecom.kafka.extract.pixel_info.serde

import java.io.{PrintWriter, StringWriter}
import java.net.URLDecoder
import java.time.{Instant, ZonedDateTime}
import java.time.ZoneOffset.UTC
import java.time.format.DateTimeFormatter.ISO_DATE_TIME

import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.pixel_info.domain.PixelInfo

import scala.collection.mutable

class PixelInfoDeserializer(appConf: Config) extends Deserializer(appConf) {
  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    val tsv = new String(payload)
    parseTsv(tsv)
  }

  private def cut_prefix(input_string: String): String = {
    val start_pos = input_string.indexOf('=')
    val cut_string = input_string.slice(start_pos + 1, input_string.length)
    cut_string
  }

  private def resolve_city(input: String): String = {
    /*sometimes citydomain is retrieved in a weired url-encoded way. if so, this'll help*/
    val regexp_pattern = """%22\D+%22"""
    val matches = regexp_pattern.r.findAllMatchIn(input).toList
    if (matches.length < 2) {
      return input
    }
    val citydomain = matches(1).toString()
    val sliced_citydomain = citydomain.slice(3, citydomain.length - 3)
    sliced_citydomain
  }

  private def split_url_querystring(url: String): mutable.Map[String, String] = {
    var map: mutable.Map[String, String] = mutable.Map.empty
    if (!url.contains('?')) return map
    val qs = url.split('?')(1)
    val qs_arr = qs.split('&')
    for (item <- qs_arr) {
      val payload = item.split("=", 2)
      val key = URLDecoder.decode(payload(0), "UTF-8")
      val value = URLDecoder.decode(payload(1), "UTF-8")
      map += (key -> value)
    }
    map
  }

  private def get_partition_values(dttm: ZonedDateTime): (Int, Int) = {
    val year = dttm.getYear
    val month = dttm.getMonthValue
    (year, month)
  }

  private def extract_timestamps(option_map: mutable.Map[String, String]): (Option[Long], Option[Long]) = {
    var result_sec: Option[Long] = Option.empty
    var result_msec: Option[Long] = Option.empty
    if (option_map.get("timestamp$c").isDefined) {
      result_sec = Option.apply(option_map("timestamp$c").toLong)
      result_msec = Option.apply(option_map("timestamp$c").toLong * 1000)
    }
    (result_sec, result_msec)
  }

  def parseTsv(tsv: String): Option[PixelInfo] = {
    val splitted = tsv.split("\t", -1)
    try {
      val zone = UTC
      val dttm = ZonedDateTime.parse(splitted(0), ISO_DATE_TIME)
      val dttm_long = dttm.toEpochSecond * 1000
      val ip = splitted(1)
      val endpoint = splitted(2)
      val ua = splitted(3)
      val uid = cut_prefix(splitted(4))
      var i = 5
      if (splitted(i).length < 1) {
        i += 1
      }
      val p_uid = cut_prefix(splitted(i))
      i += 1
      if (splitted(i).length < 1) {
        i += 1
      }
      val http_ref = split_url_querystring(cut_prefix(splitted(i)))
      val campid = http_ref.get("campId").map(_.toInt)
      val (http_ref_ts, http_ref_ts_ms) = extract_timestamps(http_ref)
      val dt_for_part = (if (http_ref_ts != Option.empty) {
        ZonedDateTime.ofInstant(Instant.ofEpochSecond(http_ref("timestamp$c").toLong), zone)
      }
      else {
        dttm
      })
      val (year, month) = get_partition_values(dt_for_part)
      val sid = http_ref.get("sid$c")
      val u = http_ref.get("u")
      val machine = http_ref.get("machine")
      val ourl = http_ref.get("ourl")
      i += 1
      var city: String = ""
      if (i <= splitted.length - 1) {
        city = resolve_city(splitted(i))
      }
      Option.apply(PixelInfo(
        datetime = dttm_long,
        ip = ip,
        pixel_endpoint = endpoint,
        user_agent = ua,
        uid = (if (uid.length < 1) {
          Option.empty
        } else {
          Option.apply(uid)
        }),
        p_uid = p_uid,
        campid = campid,
        machine = machine,
        ourl = ourl,
        u = u,
        timestamp_http_ref = http_ref_ts_ms,
        sid = sid,
        city = (if (city.length < 1) {
          Option.empty
        } else {
          Option.apply(city)
        }),
        year = year,
        month = month
      )
      )
    } catch {
      case e: Exception =>
        val sw = new StringWriter()
        e.printStackTrace(new PrintWriter(sw))
        print(sw.toString)
        Option.empty
    }
  }
}
package ru.ertelecom.kafka.extract.ac_ad_equila_s.serde

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.ac_ad_equila_s.domain.AccessLogAdEquilaS
import ru.ertelecom.kafka.extract.ac_ad_equila.serde.DESCoder
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.udf.funcs.ip.IpTransform

import scala.util.matching.Regex

class AccessLogAdEquilaSDeserializer (appConf: Config) extends Deserializer(appConf) {


  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    parseTsv(new String(payload))
  }

  def extractHttpAttrAsString(http: Option[String], rePattern: String): Option[String] = {
    val attrPattern: Regex = rePattern.r
    attrPattern.findFirstMatchIn(http.get) match {
      case Some(s) => Some(s.group(1))
      case None => None
    }
  }

  def parseTsv(tsv: String):Option[AccessLogAdEquilaS] = {
    try {

      // формат сообщения:
      // $time_iso8601#011$remote_addr#011$request_uri#011$http_user_agent#011uid=$cookie_userId#011$uid_set#011$uid_got#011
      // 0                1               2               3                4                        5           6
      // http_ref=$http_referer#011$cookie_citydomain#011$arg_u#011$arg_ourl#011$sent_http_location
      // 7                         8                     9         10           11

      val fields = tsv
        .split("#011")
        .map(_.replace("\n","").replace("\t",""))
        .map(_.trim)

      val dt = ZonedDateTime.parse(fields(0)).withZoneSameInstant(ZoneId.of("UTC"))
      val ts = dt.toEpochSecond * 1000L
      val ip = IpTransform.ip4ToLong(fields(1))

      val req_uri = Option(fields(2))
      // извлекаем кампанию из ссылки
      var campaign_id: Option[Long] = None
      var user_id: Option[String] = None
      var machine: Option[String] = None
      var sid: Option[String] = None
      if (req_uri.isDefined) {
        campaign_id = Option(extractHttpAttrAsString(req_uri, "campId%3D([^%]+)").get.toLong)
        user_id = extractHttpAttrAsString(req_uri, "u%3D([^&]+)")
        machine = extractHttpAttrAsString(req_uri, "machine%3D([^%]+)")
        sid = extractHttpAttrAsString(req_uri, "&sid=([^&]+)")
      }

      val ua = fields(3)

      val attrs = fields
        .slice(4, 8) // "uid", "p_uid", "http_ref"
        .filter(_.split('=').length > 1)
        .map(s => s.split('=')(0)->s.split("=", 2)(1))
        .filter(s => s._2 != "-")
        .toMap

      val uid = attrs.get("uid")
      val puid = attrs.get("p_uid")
      val ref = attrs.get("http_ref")

      val city_id = if (fields(8) == "-") None else Some(fields(8))

      val ourl = Option(fields(10))
      val http_location = Option(fields(11))

      val login: Option[String] = if (user_id.isDefined) Option(DESCoder.decode("dKt7G18n", user_id.get)) else None
      println(">>> login: " + login)

      Option.apply(
        AccessLogAdEquilaS(
          ts = ts,
          ip = ip,
          campaign_id = campaign_id,
          machine = machine,
          sid = sid,
          ua = ua,
          uid = uid,
          puid = puid,
          ref = ref,
          city_id = city_id,
          user_id = user_id,
          ourl = ourl,
          http_location = http_location,
          login = login,
          for_day = dt.format(DateTimeFormatter.ISO_LOCAL_DATE)
        )
      )
    } catch {
      case _: Exception => Option.empty[AccessLogAdEquilaS]
    }

  }

}

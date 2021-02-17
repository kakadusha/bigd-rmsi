package ru.ertelecom.kafka.extract.ac_ad_equila.serde

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.ac_ad_equila.domain.AccessLogAdEquila
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.udf.funcs.ip.IpTransform

//import scala.collection.immutable.Stream.Empty

class AccessLogAdEquilaDeserializer(appConf: Config) extends Deserializer(appConf) {


  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    parseTsv(new String(payload))
  }

  def parseTsv(tsv: String):Option[AccessLogAdEquila] = {
    try {

      // формат сообщения:
      // s\t$time_iso8601\t$remote_addr\t$request_uri\t$http_user_agent\tuid=$cookie_userId\t$uid_set\t$uid_got\t
      // 0  1              2             3             4                 5                   6         7
      // http_ref=$http_referer\t$cookie_citydomain\t$arg_u\t$arg_bid\t$arg_campId
      // 8                       9                   10      11        12

      val fields = tsv
        .split('\t')
        .map(_.replace("\n","").replace("\t","t"))
        .map(_.trim)

      val logSize = fields.length

      val action = fields(0)
      val dt = ZonedDateTime.parse(fields(1)).withZoneSameInstant(ZoneId.of("UTC"))
      val ts = dt.toEpochSecond * 1000L

      val ip = IpTransform.ip4ToLong(fields(2))
      val req_uri = fields(3)
      val ua = fields(4)

      val attrs = fields
        .slice(5, 9) // "uid", "p_uid", "http_ref"
        .filter(_.split('=').length > 1)
        .map(s => s.split('=')(0)->s.split("=", 2)(1))
        .filter(s => s._2 != "-")
        .toMap

      val uid = attrs.get("uid")
      val puid = attrs.get("p_uid")
      val ref = attrs.get("http_ref")

      var city_id: Option[String] = Option.empty
      if ( logSize > 8
           && (!fields(9).equals("-") || !fields(9).equals(""))
           && fields(9).split('=').length > 1
         )
        city_id = Option(fields(9))

      var user_id: Option[String] = Option.empty
      if (logSize > 10 && !fields(10).equals("-"))
        user_id = Option(fields(10))

      var billing_id: Option[Int] = Option.empty
      if (logSize > 11 && !fields(11).equals("-"))
        billing_id = Option(fields(11).toInt)

      var campaign_id: Option[Long] = Option.empty
      if (logSize > 12 && !fields(12).equals("-"))
        campaign_id = Option(fields(12).toLong)

      val login =
        if (user_id.isDefined)
          Option(DESCoder.decode("dKt7G18n", user_id.get))
        else Option.empty

      Option.apply(
        AccessLogAdEquila(
          action = action,
          ts = ts,
          ip = ip,
          req_uri = req_uri,
          ua = ua,
          uid = uid,
          puid = puid,
          ref = ref,
          city_id = city_id,
          user_id = user_id,
          billing_id = billing_id,
          campaign_id = campaign_id,
          login = login,
          for_day = dt.format(DateTimeFormatter.ISO_LOCAL_DATE)
        )
      )
    } catch {
      case _: Exception => Option.empty[AccessLogAdEquila]
    }

  }
}

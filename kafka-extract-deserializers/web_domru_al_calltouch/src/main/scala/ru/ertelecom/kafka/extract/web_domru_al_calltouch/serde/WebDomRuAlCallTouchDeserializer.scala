package ru.ertelecom.kafka.extract.web_domru_al_calltouch.serde

import java.time.format.DateTimeFormatter

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.web_domru_al_calltouch.domain.WebDomRuAlCallTouch
import ru.ertelecom.udf.funcs.ip.IpTransform
import java.net.{URI, URLDecoder, URLEncoder}
import java.time.{ZoneId, ZonedDateTime}

class WebDomRuAlCallTouchDeserializer (appConf: Config) extends Deserializer(appConf) {

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    parseTsv(new String(payload))
  }

  def parseTsv(tsv: String):Option[WebDomRuAlCallTouch] = {
    try {
      val fields = tsv.replace("\\n", "").split("\\t").map(_.trim)

      if (fields(0).trim == "s") { return Option.empty[WebDomRuAlCallTouch] }

      val dt = ZonedDateTime.parse(fields(0)).withZoneSameInstant(ZoneId.of("UTC"))
      val ip = IpTransform.ip4ToLong(fields(1))

      var uri = fields(2).replace("+", "%20")
      if (!uri.startsWith("http")) { uri = "http:/" + uri }
      val query = new URI(uri).getRawQuery
      val params = query
        .split("&")
        .filter(_.split('=').length > 1)
        .map(s => s.split("=")(0) -> URLDecoder.decode(s.split("=")(1), "UTF-8"))
        .toMap

      val timestamp = params.get("timestamp")
      var ts: Long = 0L
      if (timestamp isDefined) {
        ts = timestamp.get.toLong * 1000L
      } else {
        ts = dt.toEpochSecond * 1000L
      }


      Option.apply(
        WebDomRuAlCallTouch(
          ts = ts,
          ip = ip,
          callphase = params.get("callphase"),
          callerphone = params.get("callerphone"),
          phonenumber = params.get("phonenumber"),
          redirectNumber = params.get("redirectNumber"),
          source = params.get("source"),
          medium = params.get("medium"),
          utm_source = params.get("utm_source"),
          utm_medium = params.get("utm_medium"),
          utm_campaign = params.get("utm_campaign"),
          utm_term = params.get("utm_term"),
          gcid = params.get("gcid"),
          yaClientId = params.get("yaClientId"),
          sessionId = params.get("sessionId"),
          hostname = params.get("hostname"),
          ourl = params.get("url"),
          referer = params.get("ref"),
          user_agent = params.get("userAgent"),
          for_day = dt.format(DateTimeFormatter.ISO_LOCAL_DATE)
        )
      )
    } catch {
      case _: Exception => Option.empty[WebDomRuAlCallTouch]
    }
  }

}

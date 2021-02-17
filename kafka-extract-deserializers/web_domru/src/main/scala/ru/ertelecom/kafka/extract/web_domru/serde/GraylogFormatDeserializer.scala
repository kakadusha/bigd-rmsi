package ru.ertelecom.kafka.extract.web_domru.serde

import java.nio.charset.StandardCharsets

import scala.util.matching.Regex
import scala.util.Try
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit

import play.api.libs.json.{JsValue, Json}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.web_domru.domain.{GraylogFormat, GraylogFormatSource}
import ua_parser.{Client, Parser}
import com.google.common.cache.{Cache, CacheBuilder}


class GraylogFormatDeserializer(appConfig: Config) extends Deserializer(appConfig) with Logging {

  private lazy val uaParser = new Parser()
  lazy val uaCache: Cache[String, Client] = CacheBuilder.newBuilder()
    .maximumSize(1000000L)
    .expireAfterWrite(5 * 60, TimeUnit.SECONDS)
    .build[String, Client]
  
  lazy val isoDateTimeFmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME

  def dateTimeStringToEpoch(s: String, formatter: DateTimeFormatter): Long = 
     LocalDateTime.parse(s, formatter).toEpochSecond(ZoneOffset.UTC)

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    this.parseJson(payload)
  }

  def ipToLong(ip: String): Long = {
    val arr: Array[String] = ip.split("\\.")
    var num: Long = 0
    var i: Int = 0
    while (i < arr.length) {
      val power: Int = 3 - i
      num = num + ((arr(i).toInt % 256) * Math.pow(256, power)).toLong
      i += 1
    }
    num
  }


  def parseJson(payload: Array[Byte]): Option[GraylogFormat] = {
    try {
      val graylogFormatSource: GraylogFormatSource = Json.parse(new String(payload)).as[GraylogFormatSource]
      val uri = Try(graylogFormatSource.request.split(" ")(1)) getOrElse("-")
      val utc_ts = OffsetDateTime.parse(graylogFormatSource.timestamp).withOffsetSameInstant(ZoneOffset.UTC).toString
      val log_entry_date = utc_ts.split("T")(0)
      val ts = dateTimeStringToEpoch(utc_ts, isoDateTimeFmt)
      val remote_addr = ipToLong(graylogFormatSource.remote_addr)

      val cachedClient = uaCache.getIfPresent(graylogFormatSource.http_user_agent)
      var client: Client = null
      if (cachedClient == null) {
        client = uaParser.parse(graylogFormatSource.http_user_agent)
        uaCache.put(graylogFormatSource.http_user_agent, client)
      } else {
        client = cachedClient
      }
      val browser = client.userAgent
      var browserVersion = Array(browser.major, browser.minor, browser.patch)
        .filter(versionPiece => versionPiece != null && !versionPiece.isEmpty)
        .mkString(".")
      val browserName = if (browser.family == null || browser.family.isEmpty || browser.family == "Other") {
        Option.empty
      } else {
        Option.apply(browser.family)
      }
      if (browserVersion == null || browserVersion.isEmpty) {
        browserVersion = ""
      }

      val os = client.os
      val osName = if (os.family == null || os.family.isEmpty || os.family == "Other") Option.empty else Option.apply(os.family)
      var osVersion = Option.apply(Array(os.major, os.minor, os.patch, os.patchMinor)
        .filter(versionPiece => versionPiece != null && !versionPiece.isEmpty)
        .mkString("."))
      if ((osVersion.isDefined && osVersion.get.isEmpty) || osVersion.isEmpty) {
        osVersion = Option.empty
      }

      val deviceModel = if (
        client.device == null ||
          client.device.family == null ||
          client.device.family.isEmpty ||
          client.device.family == "Other"
      ) {
        Option.empty
      } else {
        Option.apply(client.device.family)
      }

      Option.apply(
        GraylogFormat(
          log_entry_date = log_entry_date,
          ts = ts,
          response_status = graylogFormatSource.response_status,
          remote_addr = remote_addr,
          body_bytes_sent = graylogFormatSource.body_bytes_sent,
          request_time = graylogFormatSource.request_time,
          request = graylogFormatSource.request,
          request_method = graylogFormatSource.request_method,
          host = graylogFormatSource.host,
          upstream_cache_status = graylogFormatSource.upstream_cache_status,
          upstream_addr = graylogFormatSource.upstream_addr,
          http_referrer = graylogFormatSource.http_referrer,
          upstream_response_time = Try(graylogFormatSource.upstream_response_time.toDouble) getOrElse(-1.0),
          upstream_header_time = Try(graylogFormatSource.upstream_header_time.toDouble) getOrElse(-1.0),
          upstream_connect_time = Try(graylogFormatSource.upstream_connect_time.toDouble) getOrElse(-1.0),
          request_id = graylogFormatSource.request_id,
          http_user_agent = graylogFormatSource.http_user_agent,
          cluster = graylogFormatSource.cluster,
          x_project = graylogFormatSource.x_project,
          citydomain = graylogFormatSource.citydomain,
          userId = graylogFormatSource.userId,
          p_uid = Try(graylogFormatSource.uid_set.split("=")(1)) getOrElse(graylogFormatSource.uid_got),
          request_body = graylogFormatSource.request_body,
          http_x_forwarded_for = graylogFormatSource.http_x_forwarded_for,
          lb_fqdn = graylogFormatSource.lb_fqdn,
          container_id = graylogFormatSource.container_id,
          uri = uri,
          browser_name = browserName,
          browser_version = if (browserVersion.isEmpty) Option.empty else Option.apply(browserVersion),
          os_name = osName,
          os_version = osVersion,
          device = deviceModel
        )
      )
    }
    catch {
      case e: Exception => Option.empty
    }
  }
}
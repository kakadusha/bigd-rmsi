package ru.ertelecom.kafka.extract.clickstream.serde

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import ru.ertelecom.kafka.extract.clickstream.domain.Clickstream
import ru.ertelecom.kafka.extract.clickstream.util.DpiGroupInMemoryStorage
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.udf.funcs.macs.MacTransform
import ua_parser.{Client, Parser}

class ClickstreamDeserializer(appConf: Config) extends Deserializer(appConf) {

  private lazy val df = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private lazy val uaParser = new Parser()
  private lazy val dpiGroupStorage = new DpiGroupInMemoryStorage()
  private val macPrefixes: Map[String, String] = {
    val stream = getClass.getResourceAsStream("/mac_prefixes")
    val source = scala.io.Source.fromInputStream(stream)
    source.getLines().map(line => {
      val keyVal = line.split("\t", -1)
      keyVal(0) -> keyVal(1)
    }).toMap[String, String]
  }
  private val defaultCityId: Int = -9999
  private val cookie1Name = "rambler_ruid="
  private val cookie2Name = "yandex_uid="
  private val cookie3Name = "weborama_affice="

  lazy val uaCache: Cache[String, Client] = CacheBuilder.newBuilder()
    .maximumSize(1000000L)
    .expireAfterWrite(5 * 60, TimeUnit.SECONDS)
    .build[String, Client]


  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    val tsv = new String(payload)
    parseTsv(tsv)
  }

  def parseCookie(cookieName: String, cookieSplitted: Array[String]) : Option[String] = {
    cookieSplitted
      .find(cookiePiece => cookiePiece.contains(cookieName))
      .flatMap(cookiePiece => {
        val splitted = cookiePiece.split(cookieName)
        if (splitted.length < 2) {
          Option.empty
        } else {
          Option.apply(splitted(1))
        }
      })
  }

  def parseTsv(tsv: String): Option[Clickstream] = {
    val splitted = tsv.split("\t", -1)
    try {
      val dpiGroupOpt = dpiGroupStorage.getByMachineName(splitted(1))
      if (dpiGroupOpt.isEmpty) {
        return Option.empty
      }
      val ts = splitted(2).toLong
      val dateLocal = new Timestamp(ts * 1000).toLocalDateTime.format(df)

      val cookieSplitted = if (splitted.length > 7)
        splitted(7).split('\u0059' + "|;", -1)
      else Array[String]()

      val cookie1val = parseCookie(cookie1Name, cookieSplitted)
      val cookie2val = parseCookie(cookie2Name, cookieSplitted)
      val cookie3val = parseCookie(cookie3Name, cookieSplitted)

      val ua = splitted(5)
      val cachedClient = uaCache.getIfPresent(ua)
      var client: Client = null
      if (cachedClient == null) {
        client = uaParser.parse(ua)
        uaCache.put(ua, client)
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

      var mac = if (splitted.length > 8) Option.apply(MacTransform.clearedMac(splitted(8))) else Option.empty
      if (mac.isDefined && mac.get.isEmpty) {
        mac = Option.empty
      }

      val manufacturer = mac.flatMap(m => {
        macPrefixes.get(m.substring(0, 6))
      })

      Option.apply(Clickstream(
        timestamp_local = ts * 1000,
        timestamp_utc = (ts - dpiGroupOpt.get.utcTimeDelta) * 1000,
        login = splitted(6),
        ip = splitted(3),
        url = splitted(4),
        ua = splitted(5),
        city_id = dpiGroupOpt.get.cityId,
        date_local = dateLocal,
        cookie1 = cookie1val,
        cookie2 = cookie2val,
        cookie3 = cookie3val,
        browser_name = browserName,
        browser_version = if (browserVersion.isEmpty) Option.empty else Option.apply(browserVersion),
        os_name = osName,
        os_version = osVersion,
        device = deviceModel,
        mac = mac,
        router_manufacturer = manufacturer
      ))
    } catch {
      case _: Exception => Option.empty
    }
  }
}

package ru.ertelecom.kafka.extract.web_domru_sources.serde

import java.nio.charset.StandardCharsets
import java.net.URLDecoder
import java.util.concurrent.TimeUnit
import java.net.URL

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.{OffsetDateTime, ZoneOffset}

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.web_domru_sources.domain.WebDomruSource

import ua_parser.{Client, Parser}

class WebDomruSourceDeserializer(appConf: Config) extends Deserializer(appConf) {

    private lazy val parser = new Parser
    private lazy val uaCache: Cache[String, Client] = CacheBuilder.newBuilder()
        .maximumSize(100000L)
        .expireAfterWrite(5 * 60, TimeUnit.SECONDS)
        .build[String, Client]

    lazy val isoDateTimeFmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME
    def dateTimeStringToEpoch(s: String, formatter: DateTimeFormatter): Long = 
        LocalDateTime.parse(s, formatter).toEpochSecond(ZoneOffset.UTC)

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

    def queryToMap(query: String): Map[String, String] = {
        val queryParams = query.split('?')
        val lenQueryParams = queryParams.length
        var lastElem = 1
        if (lenQueryParams > 0) {
            lastElem = lenQueryParams - 1
        }
        var params = Map.empty[String, String] 
        val params_prepare = queryParams(lastElem).replaceAll("&&","&").split('&')
        for (it <- params_prepare if it.contains("=")) {
            val m = it.split("=", 2)
            if (m(0) != null || m(0).nonEmpty || m(1) != null || m(1).nonEmpty)
                params += URLDecoder.decode(m(0), "UTF-8") -> URLDecoder.decode(m(1), "UTF-8")
        }
        return params
    }

    override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
      this.parseTsv(payload)
    }

    def parseTsv(payload: Array[Byte]): Option[WebDomruSource] = {
        try {
            val safeGetSplitGet = (L: Array[String], indexA: Int, sep: Char, indexB: Int) =>
                L.lift(indexA).map(e => e.split(sep)).flatMap(e => e.lift(indexB))

            val log = new String(payload, StandardCharsets.UTF_8).split('\t')
            val queryParams = log(2).split('?')
            val lenQueryParams = queryParams.length
            var lastElem = 1
            if (lenQueryParams > 0) {
                lastElem = lenQueryParams - 1
            } 
            var params = Map.empty[String, String] 
            val params_prepare = queryParams(lastElem).replaceAll("&&","&").split('&')
            for (it <- params_prepare if it.contains("=")) {
                val m = it.split("=", 2)
                if (m(0) != null || m(0).nonEmpty || m(1) != null || m(1).nonEmpty)
                    params += URLDecoder.decode(m(0), "UTF-8") -> URLDecoder.decode(m(1), "UTF-8")
            }
            val puid =
                if (log.length >= 6 && log(6) != "") {
                    safeGetSplitGet(log, 6, '=', 1)
                } else if (log.length >= 5 && log(5) != "") {
                    safeGetSplitGet(log, 5, '=', 1)
                } else if (log.length >= 4 && log(4) != "") {
                    safeGetSplitGet(log, 4, '=', 1)
                } else None

            val utc_ts = OffsetDateTime.parse(log(0)).withOffsetSameInstant(ZoneOffset.UTC).toString
            val log_entry_date = utc_ts.split("T")(0)
            val ts = dateTimeStringToEpoch(utc_ts, isoDateTimeFmt)

            var uaParams: Client = uaCache.getIfPresent(log(3))

            if (uaParams == null) {
                uaParams = parser.parse(log(3))
                uaCache.put(log(3), uaParams)
            }

            val ref = params.get("dr")
            val url = params.get("ploc") match {
                case Some(v) if v.nonEmpty => Option(v)
                case _ => params.get("dl")
            }

            var utm = url match {
                case Some(v) if (!v.isEmpty && v.contains("utm_source")) => queryToMap(v)
                case _ => Map.empty[String, String]
            }
            if (utm.isEmpty)
                utm = ref match {
                    case Some(q) if (!q.isEmpty && !q.contains("domru.ru") && !q.contains("ertelecom.ru")) => Map("utm_source" -> new URL(q).getHost(), "utm_medium" -> "organic")
                    case _ => Map.empty[String, String]
                }

            Option.apply(
                WebDomruSource(
                    log_entry_date = log_entry_date,
                    ts = ts,
                    p_uid = puid,
                    client_id = safeGetSplitGet(log, 4, '=', 1),
                    screenresolution = params.get("sr"),
                    ua = log(3),
                    ip = ipToLong(log(1)),
                    url = url,
                    web_id = params.get("tid"),
                    user_id = params.get("cid"),
                    ref = ref,
                    http_ref = log.lift(7).map(e => e.split("=", 2)).flatMap(e => e.lift(1)),
                    gtm = params.get("gtm").map(_.drop(6)),
                    citydomain = if (log.length > 8) Option(log(8)) else None,
                    browser_name = Option(uaParams.userAgent.family),
                    browser_version = Option(
                        Array(uaParams.userAgent.major, uaParams.userAgent.minor, uaParams.userAgent.patch)
                            .filter(x => x != null && x != "").mkString(".")
                    ),
                    os_name = Option(uaParams.os.family),
                    os_version = Option(
                        Array(uaParams.os.major, uaParams.os.minor, uaParams.os.patch)
                            .filter(x => x != null && x != "").mkString(".")
                    ),
                    device = Option(uaParams.device.family),
                    utm_source = utm.get("utm_source").get,
                    utm_medium = utm.get("utm_medium").get,
                    utm_campaign = utm.get("utm_campaign"),
                    utm_content = utm.get("utm_content"),
                    utm_term = utm.get("utm_term"),
                    gclid = utm.get("gclid"),
                    yclid = utm.get("yclid"),
                    equila_city_id = utm.get("equila_city_id")
                )
            )
        } catch {
            case e: Exception => Option.empty
        }
    }
}


package ru.ertelecom.kafka.extract.pixel_wifi.serde

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.net.URLDecoder
import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ua_parser.{Client, Parser}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.pixel_wifi.domain.PixelDomRuWifi


class PixelDomRuWifiDeserializer(appConf: Config) extends Deserializer(appConf) {
    private lazy val KEY = "6QR9zuSdN4"
    private lazy val parser = new Parser
    private lazy val uaCache: Cache[String, Client] = CacheBuilder.newBuilder()
      .maximumSize(100000L)
      .expireAfterWrite(5 * 60, TimeUnit.SECONDS)
      .build[String, Client]


    override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
        this.parseTsv(payload)
    }

    def decodeXor(encodedText: String, key: String): String = {
        val keyBytes = key.toArray.map(_.toByte)
        val keyLen   = keyBytes.length
        val bytes64  = Base64.decodeBase64(encodedText)
        bytes64.zipWithIndex.map {
            case (b, i) => (b ^ keyBytes(i % keyLen)).toChar
        }.mkString
    }

    def parseTsv(payload: Array[Byte]): Option[PixelDomRuWifi] = {

        val safeGetSplitGet = (L: Array[String], indexA: Int, sep: Char, indexB: Int) =>
            L.lift(indexA).map(e => e.split(sep)).flatMap(e => e.lift(indexB))

        try {
            val log            = new String(payload, StandardCharsets.UTF_8).split('\t')
            val datehourminute = OffsetDateTime.parse(log(0), DateTimeFormatter.ISO_OFFSET_DATE_TIME)

            val uriList = log(2).split('?')
            val uri = if (uriList.length > 1)
                        uriList(1)
                      else return Option.empty[PixelDomRuWifi]
            val params = decodeXor(uri, KEY).split('&').map(v => {
                val m = v.split("=", 2).map(s => URLDecoder.decode(s, "UTF-8"))
                m(0) -> m(1)
            }).filter(_._2 != "null").toMap

            val ts:Long = (params.get("Timestamp") match {
                case Some(t) => t
                case None    => return Option.empty[PixelDomRuWifi]
            }).toLong



            val puid =
                if (log.length >= 6 && log(6) != "") {
                    safeGetSplitGet(log, 6, '=', 1)
                } else if (log.length >= 5 && log(5) != "") {
                    safeGetSplitGet(log, 5, '=', 1)
                } else if (log.length >= 4 && log(4) != "") {
                    safeGetSplitGet(log, 4, '=', 1)
                } else None

            var uaParams: Client = uaCache.getIfPresent(log(3))

            if(uaParams == null) {
              uaParams = parser.parse(log(3))
              uaCache.put(log(3), uaParams)
            }

            Option.apply(
                PixelDomRuWifi(
                    loginid =  params.get("LoginId"),
                    acctid = params.get("AcctId"),
                    deviceip = params.get("DeviceIP"),
                    devicemac = params.get("DeviceMac"),
                    locationid = params.get("LocationId"),
                    pageid = params.get("PageId").getOrElse(""),
                    routerid = params.get("RouterId"),
                    clientts = ts * 1000,
                    datehourminute = datehourminute.toInstant.toEpochMilli,
                    client_id = safeGetSplitGet(log, 4, '=', 1),
                    ua = log(3),
                    ip = log(1).filterNot((x: Char) => x.isWhitespace),
                    puid = puid,
                    http_ref = log.lift(7).map(e => e.split("=",2)).flatMap(e => e.lift(1)),
                    citydomain = if (log.length > 8) Option(log(8)) else None,
                    userid = params.get("UserId"),
                    vkid = params.get("VkId"),
                    okid = params.get("OkId"),
                    fbid = params.get("FbId"),
                    twid = params.get("twid"),
                    instid = params.get("InstId"),
                    cityid = params.get("CityId"),
                    browser_name = Option(uaParams.userAgent.family),
                    browser_version =Option(
                        Array(uaParams.userAgent.major, uaParams.userAgent.minor, uaParams.userAgent.patch)
                          .filter(x => x != null && x != "").mkString(".")
                    ),
                    os_name = Option(uaParams.os.family),
                    os_version = Option(
                        Array(uaParams.os.major, uaParams.os.minor, uaParams.os.patch)
                          .filter(x => x != null && x != "").mkString(".")
                    ),
                    device = Option(uaParams.device.family),
                    language = params.get("Language"),
                    month = datehourminute.getMonthValue,
                    year = datehourminute.getYear
                )
            )

        } catch {
            case _: Exception => Option.empty[PixelDomRuWifi]
        }
    }
}


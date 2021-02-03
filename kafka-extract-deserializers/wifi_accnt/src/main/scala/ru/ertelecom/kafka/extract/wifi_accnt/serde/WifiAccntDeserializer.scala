package ru.ertelecom.kafka.extract.wifi_accnt.serde

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.math.abs
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import play.api.libs.json.{JsSuccess, Json}
import com.google.common.cache.{Cache, CacheBuilder}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.wifi_accnt.domain.{WifiAccnt, WifiAccntSource}

import scala.util.matching.Regex

class WifiAccntDeserializer(appConf: Config) extends Deserializer(appConf) {
  lazy val preDedupeCache: Cache[java.lang.String, java.lang.Long] = CacheBuilder.newBuilder()
    .maximumSize(1000000L)
    .expireAfterWrite(10, TimeUnit.SECONDS)
    .expireAfterAccess(10, TimeUnit.SECONDS)
    .build[java.lang.String, java.lang.Long]()

  private final val MAC_REGEXP = "^([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})$"

  private final val VLAN_REGEXP = "vlanid=(\\d{1,4});vlanid2=(\\d{1,4});"
  private val pattern_vlan = Pattern.compile(VLAN_REGEXP)

  private final val TS_DELTA = 2

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    this.parseJson(payload)
  }.asNondeterministic()

  def reformatMac(raw_mac: String): String = {
    var mac: String = ""
    val keyValPattern = new Regex(MAC_REGEXP)
    keyValPattern.findAllIn(raw_mac).matchData foreach { m =>
      val parts = new Array[String](m.groupCount)
      for (i <- 1 to m.groupCount) {
        parts(i - 1) = m.group(i)
      }
      mac = parts.mkString(":")
    }
    if (mac.isEmpty) {
      mac = raw_mac
    }
    mac
  }

  def getAuthid(un: String, callstationid: String): Int = {
    var auth_id = 5
    if (un.nonEmpty && callstationid.nonEmpty) {
      val formatted_un = un.replaceAllLiterally(":", "")
      val formatted_csid = callstationid.replaceAllLiterally(":", "").replaceAllLiterally(".", "")
      if (formatted_un == formatted_csid) {
        auth_id = 1
      } // неавторизованный пользователь
      else if (formatted_un.matches(".*@wifi$")) {
        auth_id = 7
      } // гость
      else if (formatted_un.matches(".*@wifi_pa$")) {
        auth_id = 6
      } // премиум аккаунт
      else auth_id = 5 // абонент домру
    }
    auth_id
  }


  def parseVlan(nasportid: String): String = {
    val matcher = pattern_vlan.matcher(nasportid)
    val vlans = new Array[String](2)
    while ( {
      matcher.find
    }) for (i <- 1 until 3) {
      vlans(i - 1) = matcher.group(i)
    }
    vlans.mkString(":")
  }

  def getVlan(nasportid: String): String = {
    var vlan = nasportid
    if (nasportid.matches(".*vlanid.*")) vlan = parseVlan(nasportid)
    else if (nasportid.matches(".*lag-.*") || nasportid.matches("bng.*")) vlan = nasportid.split(":")(1).replace(".", ":")
    else if (nasportid.matches(".*pw.*")) {
      val splitted = nasportid.split("-")
      if (splitted.length >= 2)
        vlan = splitted(1)
      else
        vlan = "1"
    }
    else if (nasportid.matches("\\d{1,4}.\\d{1,4}")) vlan = nasportid.replace(".", ":")
    else vlan = "1"
    vlan
  }

  def checkVlan(nasportid: Option[String]): String = {
    nasportid match {
      case Some(value) => getVlan(value)
      case _ => "1"
    }
  }

  def checkWifiRow(payload: WifiAccntSource): Boolean = {
    (payload.wifisub == 1 || payload.bid == 7777) && payload.astype == 2
  }

  def checkCache(payload: WifiAccntSource): Boolean = {
    val cache_key = payload.un + payload.asid + payload.astype.toString + payload.wifisub.toString + payload.bid.toString
    val cache_stored_ts: java.lang.Long = preDedupeCache.getIfPresent(cache_key)
    cache_stored_ts == null || abs(payload.eventts - cache_stored_ts) >= TS_DELTA
  }

  def getRow(payload: String): Option[WifiAccntSource] = {
    val jsRow = Json.parse(payload).validate[WifiAccntSource]
    jsRow match {
      case JsSuccess(value, _) => Option.apply(value)
      case _ => Option.empty
    }
  }

  def rowToWifiAccnt(row: WifiAccntSource): WifiAccnt = {
    val cache_key = row.un + row.asid + row.astype.toString + row.wifisub.toString + row.bid.toString
    preDedupeCache.put(cache_key, row.eventts)
    WifiAccnt(
      reformatMac(row.un),
      row.nasid,
      row.astype,
      row.asid,
      row.aioct,
      row.aigw,
      row.aooct,
      row.aogw,
      row.callstationid,
      row.framedip,
      row.astime,
      row.eventts,
      row.adslacid,
      row.alcslaprof,
      row.atcause,
      row.amsessionid,
      row.nasportid,
      row.nasip,
      row.nasporttype.toString,
      row.hwidname,
      row.hwipname,
      row.alcclienthw,
      row.wifisub,
      getAuthid(row.un, row.callstationid.getOrElse("")),
      checkVlan(row.nasportid),
      row.bid,
      row.astime,
      4294967296L * row.aigw + row.aioct * 1L,
      4294967296L * row.aogw + row.aooct * 1L,
      row.eventts - row.astime,
      row.eventts
    )
  }

  def parseJson(payload: Array[Byte]): Option[WifiAccnt] = {
    val srcRow = getRow(new String(payload))
    val filteredRow: Option[WifiAccntSource] = srcRow.filter(checkWifiRow)
    val wifiAccntRow = filteredRow.filter(checkCache)
    wifiAccntRow.map(rowToWifiAccnt)
  }
}

package ru.ertelecom.kafka.extract.wifi_ruckus_syslog.serde

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
import ru.ertelecom.kafka.extract.wifi_ruckus_syslog.domain.{RuckusSyslogSource, RuckusMessageSource, RuckusSyslog}


class RuckusSyslogDeserializer(appConfig: Config) extends Deserializer(appConfig) with Logging {

    lazy val isoDateTimeFmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME

    def dateTimeStringToEpoch(s: String, formatter: DateTimeFormatter): Long = 
        LocalDateTime.parse(s, formatter).toEpochSecond(ZoneOffset.UTC)

    override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
        this.parseJson(payload)
    }

    def toInt(s: Option[String]): Option[Int] = s match {
        case None => None
        case Some(v) if v.isEmpty  => None
        case Some(v) => if (v.forall(_.isDigit) || (v.startsWith("-") && v.drop(1).forall(_.isDigit)))
                Some(v.toInt)
            else
                None
    }

    def toLong(s: Option[String]): Option[Long] = s match {
        case None => None
        case Some(v) if v.isEmpty  => None
        case Some(v) => if (v.forall(_.isDigit) || (v.startsWith("-") && v.drop(1).forall(_.isDigit)))
                Some(v.toLong)
            else
                None
    }

    def toDateTime(s: Option[String]): Option[Long] = s match {
        case None => None
        case Some(v) if v.isEmpty  => None
        case Some(v) => if ((v.forall(_.isDigit) || (v.startsWith("-") && v.drop(1).forall(_.isDigit))) && v.toLong != 0l)
                Some(v.toLong)
            else
                None
    }

    def ipToLong(ip: String): Long = {
        if (!ip.isEmpty) {
                    val arr: Array[String] = ip.split("\\.")
          var num: Long = 0
          var i: Int = 0
          while (i < arr.length) {
          val power: Int = 3 - i
          num = num + ((arr(i).toInt % 256) * Math.pow(256, power)).toLong
          i += 1
          }
          num
        } else {
          0L
        }
    }

    def reformat_mac(raw_mac: String): StringBuilder = {
        val mac = new StringBuilder()
        val keyValPattern = new Regex("^([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})$")
        keyValPattern.findAllIn(raw_mac.toUpperCase()).matchData foreach {
        m => mac.append(m.group(1) + m.group(2) + m.group(3) + m.group(4) + m.group(5) + m.group(6))
        }
        return mac
    }

    def parseJson(payload: Array[Byte]): Option[RuckusSyslog] = {
        try {
            val ruckusSyslogSource: RuckusSyslogSource = Json.parse(new String(payload)).as[RuckusSyslogSource]

            val utc_ts = OffsetDateTime.parse(ruckusSyslogSource.timestamp).withOffsetSameInstant(ZoneOffset.UTC).toString
            val log_entry_date = utc_ts.split("T")(0)
            val ts = dateTimeStringToEpoch(utc_ts, isoDateTimeFmt)
            val event_type = ruckusSyslogSource.message.split(",")(1)

            val startJsonString:Int = ruckusSyslogSource.message.indexOf('\"')
            val jsonString:String = ruckusSyslogSource.message.substring(startJsonString).replaceAll("=", ":").replaceAll("\\\\","")
            val messageJson:String = s"{$jsonString}"
            val ruckusMessageSource: RuckusMessageSource = Json.parse(messageJson).as[RuckusMessageSource]

            Option.apply(
                RuckusSyslog(
                    log_entry_date = log_entry_date,
                    ts = ts,
                    ssid = ruckusMessageSource.ssid,
                    event_type = event_type,
                    ap_mac = reformat_mac(ruckusMessageSource.apMac).toString,
                    client_mac = reformat_mac(ruckusMessageSource.clientMac).toString,
                    client_ip = if (ruckusMessageSource.clientIP == null && ruckusMessageSource.clientIP.isEmpty) Option.empty else Option.apply(ipToLong(ruckusMessageSource.clientIP.getOrElse(""))),
                    username = if (ruckusMessageSource.userName == null && ruckusMessageSource.userName.isEmpty) Option.empty else ruckusMessageSource.userName,
                    vlan = ruckusMessageSource.vlanId.toInt,
                    client_os = if (ruckusMessageSource.osType == null && ruckusMessageSource.osType.isEmpty) Option.empty else ruckusMessageSource.osType,
                    client_hostname = if (ruckusMessageSource.hostname == null && ruckusMessageSource.hostname.isEmpty) Option.empty else ruckusMessageSource.hostname,
                    radio = ruckusMessageSource.radio,
                    first_auth = toDateTime(ruckusMessageSource.firstAuth),
                    association_time = toDateTime(ruckusMessageSource.associationTime),
                    ip_assign_time = toDateTime(ruckusMessageSource.ipAssignTime),
                    disconnect_time = toDateTime(ruckusMessageSource.disconnectTime),
                    session_duration = toDateTime(ruckusMessageSource.sessionDuration),
                    disconnect_reason = toInt(ruckusMessageSource.disconnectReason),
                    rx_bytes = toLong(ruckusMessageSource.rxBytes),
                    tx_bytes = toLong(ruckusMessageSource.txBytes),
                    peak_rx = toInt(ruckusMessageSource.peakRx),
                    peak_tx = toInt(ruckusMessageSource.peakTx),
                    rssi = toInt(ruckusMessageSource.rssi),
                    received_signal_strength = toInt(ruckusMessageSource.receivedSignalStrength),
                    zone_name = ruckusMessageSource.zoneName,
                    ap_location = if (ruckusMessageSource.apLocation == null && ruckusMessageSource.apLocation.isEmpty) Option.empty else ruckusMessageSource.apLocation,
                    ap_gps = if (ruckusMessageSource.apGps == null && ruckusMessageSource.apGps.isEmpty) Option.empty else ruckusMessageSource.apGps,
                    ap_ip = ipToLong(ruckusMessageSource.apIpAddress),
                    ap_desc = if (ruckusMessageSource.apDescription == null && ruckusMessageSource.apDescription.isEmpty) Option.empty else ruckusMessageSource.apDescription,
                    to_radio = if (ruckusMessageSource.toRadio == null && ruckusMessageSource.toRadio.isEmpty) Option.empty else ruckusMessageSource.toRadio,
                    from_ap_mac = if (ruckusMessageSource.fromApMac == null && ruckusMessageSource.fromApMac.isEmpty) Option.empty else ruckusMessageSource.fromApMac
                )
            )
        } catch {
            case e: Exception => Option.empty
        }
    }
}
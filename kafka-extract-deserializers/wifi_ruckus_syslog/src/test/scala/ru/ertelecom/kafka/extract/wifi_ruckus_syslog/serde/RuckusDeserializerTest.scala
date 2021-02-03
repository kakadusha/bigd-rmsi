package ru.ertelecom.kafka.extract.wifi_ruckus_syslog.serde

import java.nio.charset.StandardCharsets
import org.scalatest.FunSuite
import play.api.libs.json.{JsValue, Json}
import ru.ertelecom.kafka.extract.wifi_ruckus_syslog.domain.{RuckusSyslogSource, RuckusMessageSource, RuckusSyslog}

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.{OffsetDateTime, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.util.matching.Regex

class RuckusDeserializerTest extends FunSuite {

    val payload = """
      {"timestamp":"2020-06-02T15:27:17+03:00","message":" @@204,clientDisconnect,\"apMac\"=\"0c:f4:d5:2b:8f:90\",\"clientMac\"=\"04:8c:9a:b5:e7:b4\",\"ssid\"=\"DOM.RU Wi-Fi\",\"bssid\"=\"0c:f4:d5:6b:8f:98\",\"userId\"=\"\",\"wlanId\"=\"13\",\"iface\"=\"wlan1\",\"tenantUUID\"=\"839f87c6-d116-497e-afce-aa8157abd30c\",\"apName\"=\"MSK-AP4494\",\"clientIP\"=\"10.15.38.10\",\"vlanId\"=\"3618\",\"radio\"=\"g\",\"encryption\"=\"None\",\"osType\"=\"Android\",\"hostname\"=\"HONOR_10i-79a37eee9add233\",\"firstAuth\"=\"1591111635\",\"associationTime\"=\"1591111635\",\"ipAssignTime\"=\"1591111635\",\"disconnectTime\"=\"1591111636\",\"sessionDuration\"=\"1\",\"disconnectReason\"=\"3\",\"rxFrames\"=\"14\",\"rxBytes\"=\"1645\",\"txFrames\"=\"9\",\"txBytes\"=\"1662\",\"peakRx\"=\"1645\",\"peakTx\"=\"1662\",\"rssi\"=\"36\",\"receivedSignalStrength\"=\"-76\",\"Instantaneous rssi\"=\"0\",\"Xput\"=\"0\",\"fwVersion\"=\"5.0.0.0.760\",\"model\"=\"T301S\",\"zoneUUID\"=\"1379ebf0-0f72-49c9-bab9-98c081bb9811\",\"zoneName\"=\"DIT\",\"timeZone\"=\"MSK-3\",\"apLocation\"=\"ул. Золоторожский Вал, 32\",\"apGps\"=\"\",\"apIpAddress\"=\"10.90.189.146\",\"apIpv6Address\"=\"\",\"apGroupUUID\"=\"69bb0e09-ec75-4c05-b3a5-04fbd2f6a71c\",\"domainId\"=\"0cdbfb45-0ffe-4ded-986c-a6154a03b^C109\",\"serialNumber\"=\"111704000019\",\"wlanGroupUUID\"=\"316b9b42-9ebe-11e7-92a8-000c29950cfc\",\"idealEventVersion\"=\"3.5.1\",\"apDescription\"=\"столб; 6м; 280; 0; CityWiFi\"","host":"vsz2-cs-mr-msk","severity":"info","facility":"local0","syslog-tag":"Core:"}
    """.stripMargin.getBytes

    val payload_mt = """
      {"timestamp":"2020-06-16T15:30:17+03:00","message":" @@202,clientJoin,\"apMac\"=\"ec:8c:a2:0f:01:80\",\"clientMac\"=\"04:ba:8d:6c:c5:cd\",\"ssid\"=\"MT_FREE\",\"bssid\"=\"ec:8c:a2:4f:01:88\",\"userId\"=\"\",\"wlanId\"=\"152\",\"iface\"=\"wlan1\",\"tenantUUID\"=\"839f87c6-d116-497e-afce-aa8157abd30c\",\"apName\"=\"MSK-AP0857\",\"vlanId\"=\"109\",\"radio\"=\"g/n\",\"encryption\"=\"None\",\"Instantaneous rssi\"=\"0\",\"Xput\"=\"0\",\"fwVersion\"=\"5.0.0.0.760\",\"model\"=\"T301S\",\"zoneUUID\"=\"1379ebf0-0f72-49c9-bab9-98c081bb9811\",\"zoneName\"=\"DIT\",\"timeZone\"=\"MSK-3\",\"apLocation\"=\"Петровка, д. 25/2\",\"apGps\"=\"\",\"apIpAddress\"=\"172.26.147.234\",\"apIpv6Address\"=\"\",\"apGroupUUID\"=\"9ed1262e-0b8e-4286-ac8c-c4d96752689a\",\"domainId\"=\"0cdbfb45-0ffe-4ded-986c-a6154a03b109\",\"serialNumber\"=\"271604104625\",\"wlanGroupUUID\"=\"b15df851-b480-11e9-985d-420aa6cc7ccf\",\"idealEventVersion\"=\"3.5.1\",\"apDescription\"=\"Петровка, д. 25/2;ограждение кровли;H 12м;Угол -3;Az340;CityWiFi\"","host":"vsz1-cs-m9-msk","severity":"info","facility":"local0","syslog-tag":"Core:"}
    """.stripMargin.getBytes

    val payload_20200710 = """{"timestamp":"2020-07-09T20:19:33+03:00","message":" @@205,clientInactivityTimeout,\"apMac\"=\"ec:8c:a2:0f:ff:90\",\"clientMac\"=\"d4:63:c6:94:5a:e2\",\"ssid\"=\"MT_FREE\",\"bssid\"=\"ec:8c:a2:cf:ff:98\",\"userId\"=\"\",\"wlanId\"=\"152\",\"iface\"=\"wlan3\",\"tenantUUID\"=\"839f87c6-d116-497e-afce-aa8157abd30c\",\"apName\"=\"MSK-AP3018\",\"vlanId\"=\"109\",\"radio\"=\"g/n\",\"encryption\"=\"None\",\"firstAuth\"=\"1594325835\",\"associationTime\"=\"1594325835\",\"ipAssignTime\"=\"0\",\"disconnectTime\"=\"1594325974\",\"sessionDuration\"=\"139\",\"disconnectReason\"=\"4\",\"rxFrames\"=\"1\",\"rxBytes\"=\"56\",\"txFrames\"=\"3\",\"txBytes\"=\"28\",\"peakRx\"=\"56\",\"peakTx\"=\"28\",\"rssi\"=\"21\",\"receivedSignalStrength\"=\"-91\",\"Instantaneous rssi\"=\"0\",\"Xput\"=\"0\",\"fwVersion\"=\"5.0.0.0.760\",\"model\"=\"T301N\",\"zoneUUID\"=\"1379ebf0-0f72-49c9-bab9-98c081bb9811\",\"zoneName\"=\"DIT\",\"timeZone\"=\"MSK-3\",\"apLocation\"=\"ул Никитская Б., д. 23, 25, 29, 36\",\"apGps\"=\"\",\"apIpAddress\"=\"10.190.189.46\",\"apIpv6Address\"=\"\",\"apGroupUUID\"=\"9ed1262e-0b8e-4286-ac8c-c4d96752689a\",\"domainId\"=\"0cdbfb45-0ffe-4ded-986c-a6154a03b109\",\"serialNumber\"=\"291604400076\",\"wlanGroupUUID\"=\"b15df851-b480-11e9-985d-420aa6cc7ccf\",\"idealEventVersion\"=\"3.5.1\",\"apDescription\"=\"столб; 3м; 58; 0; CityWiFi\"","host":"vsz2-cs-mr-msk","severity":"info","facility":"local0","syslog-tag":"Core:"}
    """.stripMargin.getBytes

    val message = " @@204,clientDisconnect,\"apMac\"=\"0c:f4:d5:2b:8f:90\",\"clientMac\"=\"04:8c:9a:b5:e7:b4\",\"ssid\"=\"DOM.RU Wi-Fi\",\"bssid\"=\"0c:f4:d5:6b:8f:98\",\"userId\"=\"\",\"wlanId\"=\"13\",\"iface\"=\"wlan1\",\"tenantUUID\"=\"839f87c6-d116-497e-afce-aa8157abd30c\",\"apName\"=\"MSK-AP4494\",\"clientIP\"=\"10.15.38.10\",\"vlanId\"=\"3618\",\"radio\"=\"g\",\"encryption\"=\"None\",\"osType\"=\"Android\",\"hostname\"=\"HONOR_10i-79a37eee9add233\",\"firstAuth\"=\"1591111635\",\"associationTime\"=\"1591111635\",\"ipAssignTime\"=\"1591111635\",\"disconnectTime\"=\"1591111636\",\"sessionDuration\"=\"1\",\"disconnectReason\"=\"3\",\"rxFrames\"=\"14\",\"rxBytes\"=\"1645\",\"txFrames\"=\"9\",\"txBytes\"=\"1662\",\"peakRx\"=\"1645\",\"peakTx\"=\"1662\",\"rssi\"=\"36\",\"receivedSignalStrength\"=\"-76\",\"Instantaneous rssi\"=\"0\",\"Xput\"=\"0\",\"fwVersion\"=\"5.0.0.0.760\",\"model\"=\"T301S\",\"zoneUUID\"=\"1379ebf0-0f72-49c9-bab9-98c081bb9811\",\"zoneName\"=\"DIT\",\"timeZone\"=\"MSK-3\",\"apLocation\"=\"ул. Золоторожский Вал, 32\",\"apGps\"=\"\",\"apIpAddress\"=\"10.90.189.146\",\"apIpv6Address\"=\"\",\"apGroupUUID\"=\"69bb0e09-ec75-4c05-b3a5-04fbd2f6a71c\",\"domainId\"=\"0cdbfb45-0ffe-4ded-986c-a6154a03b^C109\",\"serialNumber\"=\"111704000019\",\"wlanGroupUUID\"=\"316b9b42-9ebe-11e7-92a8-000c29950cfc\",\"idealEventVersion\"=\"3.5.1\",\"apDescription\"=\"столб; 6м; 280; 0; CityWiFi\""

    lazy val isoDateTimeFmt = DateTimeFormatter.ISO_OFFSET_DATE_TIME

    def dateTimeStringToEpoch(s: String, formatter: DateTimeFormatter): Long = 
        LocalDateTime.parse(s, formatter).toEpochSecond(ZoneOffset.UTC)

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
  test("RuckusSyslogSource test") {
    
    val ruckusSyslogSource: RuckusSyslogSource = Json.parse(new String(payload)).as[RuckusSyslogSource]

    assert("2020-06-02T15:27:17+03:00" == ruckusSyslogSource.timestamp)
    assert("local0" == ruckusSyslogSource.facility)
    assert(message == ruckusSyslogSource.message)
    assert("clientDisconnect" == ruckusSyslogSource.message.split(",")(1))
  }

  test("RuckusSyslogSource test MT_FREE") {
    
    val ruckusSyslogSource: RuckusSyslogSource = Json.parse(new String(payload_mt)).as[RuckusSyslogSource]

    assert("2020-06-16T15:30:17+03:00" == ruckusSyslogSource.timestamp)
    assert("local0" == ruckusSyslogSource.facility)
    //assert(message == ruckusSyslogSource.message)
    assert("clientJoin" == ruckusSyslogSource.message.split(",")(1))
  }

  test("RuckusMessageSource test") {
    val startJsonString:Int = message.indexOf('\"')
    assert(24 == startJsonString)

    val jsonString:String = message.substring(startJsonString).replaceAll("=", ":").replaceAll("\\\\","")
    assert("\"apMac\":\"0c:f4:d5:2b:8f:90\",\"clientMac\":\"04:8c:9a:b5:e7:b4\",\"ssid\":\"DOM.RU Wi-Fi\",\"bssid\":\"0c:f4:d5:6b:8f:98\",\"userId\":\"\",\"wlanId\":\"13\",\"iface\":\"wlan1\",\"tenantUUID\":\"839f87c6-d116-497e-afce-aa8157abd30c\",\"apName\":\"MSK-AP4494\",\"clientIP\":\"10.15.38.10\",\"vlanId\":\"3618\",\"radio\":\"g\",\"encryption\":\"None\",\"osType\":\"Android\",\"hostname\":\"HONOR_10i-79a37eee9add233\",\"firstAuth\":\"1591111635\",\"associationTime\":\"1591111635\",\"ipAssignTime\":\"1591111635\",\"disconnectTime\":\"1591111636\",\"sessionDuration\":\"1\",\"disconnectReason\":\"3\",\"rxFrames\":\"14\",\"rxBytes\":\"1645\",\"txFrames\":\"9\",\"txBytes\":\"1662\",\"peakRx\":\"1645\",\"peakTx\":\"1662\",\"rssi\":\"36\",\"receivedSignalStrength\":\"-76\",\"Instantaneous rssi\":\"0\",\"Xput\":\"0\",\"fwVersion\":\"5.0.0.0.760\",\"model\":\"T301S\",\"zoneUUID\":\"1379ebf0-0f72-49c9-bab9-98c081bb9811\",\"zoneName\":\"DIT\",\"timeZone\":\"MSK-3\",\"apLocation\":\"ул. Золоторожский Вал, 32\",\"apGps\":\"\",\"apIpAddress\":\"10.90.189.146\",\"apIpv6Address\":\"\",\"apGroupUUID\":\"69bb0e09-ec75-4c05-b3a5-04fbd2f6a71c\",\"domainId\":\"0cdbfb45-0ffe-4ded-986c-a6154a03b^C109\",\"serialNumber\":\"111704000019\",\"wlanGroupUUID\":\"316b9b42-9ebe-11e7-92a8-000c29950cfc\",\"idealEventVersion\":\"3.5.1\",\"apDescription\":\"столб; 6м; 280; 0; CityWiFi\"" == jsonString)

    val messageJson:String = s"{$jsonString}"
    assert("{\"apMac\":\"0c:f4:d5:2b:8f:90\",\"clientMac\":\"04:8c:9a:b5:e7:b4\",\"ssid\":\"DOM.RU Wi-Fi\",\"bssid\":\"0c:f4:d5:6b:8f:98\",\"userId\":\"\",\"wlanId\":\"13\",\"iface\":\"wlan1\",\"tenantUUID\":\"839f87c6-d116-497e-afce-aa8157abd30c\",\"apName\":\"MSK-AP4494\",\"clientIP\":\"10.15.38.10\",\"vlanId\":\"3618\",\"radio\":\"g\",\"encryption\":\"None\",\"osType\":\"Android\",\"hostname\":\"HONOR_10i-79a37eee9add233\",\"firstAuth\":\"1591111635\",\"associationTime\":\"1591111635\",\"ipAssignTime\":\"1591111635\",\"disconnectTime\":\"1591111636\",\"sessionDuration\":\"1\",\"disconnectReason\":\"3\",\"rxFrames\":\"14\",\"rxBytes\":\"1645\",\"txFrames\":\"9\",\"txBytes\":\"1662\",\"peakRx\":\"1645\",\"peakTx\":\"1662\",\"rssi\":\"36\",\"receivedSignalStrength\":\"-76\",\"Instantaneous rssi\":\"0\",\"Xput\":\"0\",\"fwVersion\":\"5.0.0.0.760\",\"model\":\"T301S\",\"zoneUUID\":\"1379ebf0-0f72-49c9-bab9-98c081bb9811\",\"zoneName\":\"DIT\",\"timeZone\":\"MSK-3\",\"apLocation\":\"ул. Золоторожский Вал, 32\",\"apGps\":\"\",\"apIpAddress\":\"10.90.189.146\",\"apIpv6Address\":\"\",\"apGroupUUID\":\"69bb0e09-ec75-4c05-b3a5-04fbd2f6a71c\",\"domainId\":\"0cdbfb45-0ffe-4ded-986c-a6154a03b^C109\",\"serialNumber\":\"111704000019\",\"wlanGroupUUID\":\"316b9b42-9ebe-11e7-92a8-000c29950cfc\",\"idealEventVersion\":\"3.5.1\",\"apDescription\":\"столб; 6м; 280; 0; CityWiFi\"}" == messageJson)
    
    val ruckusMessageSource: RuckusMessageSource = Json.parse(messageJson).as[RuckusMessageSource]
    assert("0c:f4:d5:2b:8f:90" == ruckusMessageSource.apMac)
  }

  test("RuckusSyslog test") {
    val ruckusSyslogSource: RuckusSyslogSource = Json.parse(new String(payload)).as[RuckusSyslogSource]
    val startJsonString:Int = ruckusSyslogSource.message.indexOf('\"')
    val jsonString:String = ruckusSyslogSource.message.substring(startJsonString).replaceAll("=", ":").replaceAll("\\\\","")
    val messageJson:String = s"{$jsonString}"
    val ruckusMessageSource: RuckusMessageSource = Json.parse(messageJson).as[RuckusMessageSource]

    val utc_ts = OffsetDateTime.parse(ruckusSyslogSource.timestamp).withOffsetSameInstant(ZoneOffset.UTC).toString
    val ts = dateTimeStringToEpoch(utc_ts, isoDateTimeFmt)
    val log_entry_date = utc_ts.split("T")(0)
    assert("2020-06-02" == log_entry_date)

    val event_type = ruckusSyslogSource.message.split(",")(1)
    assert("clientDisconnect" == event_type)

    val ruckusSyslog = RuckusSyslog(
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
        first_auth = toLong(ruckusMessageSource.firstAuth),
        association_time = toLong(ruckusMessageSource.associationTime),
        ip_assign_time = toLong(ruckusMessageSource.ipAssignTime),
        disconnect_time = toLong(ruckusMessageSource.disconnectTime),
        session_duration = toLong(ruckusMessageSource.sessionDuration),
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
    
    assert("048C9AB5E7B4" == ruckusSyslog.client_mac)
    assert(Some(1591111635) == ruckusSyslog.association_time)
  }

  test("RuckusSyslog test MT_FREE") {
    val ruckusSyslogSource: RuckusSyslogSource = Json.parse(new String(payload_mt)).as[RuckusSyslogSource]
    val startJsonString:Int = ruckusSyslogSource.message.indexOf('\"')
    val jsonString:String = ruckusSyslogSource.message.substring(startJsonString).replaceAll("=", ":").replaceAll("\\\\","")
    val messageJson:String = s"{$jsonString}"
    val ruckusMessageSource: RuckusMessageSource = Json.parse(messageJson).as[RuckusMessageSource]

    val utc_ts = OffsetDateTime.parse(ruckusSyslogSource.timestamp).withOffsetSameInstant(ZoneOffset.UTC).toString
    val ts = dateTimeStringToEpoch(utc_ts, isoDateTimeFmt)
    val log_entry_date = utc_ts.split("T")(0)
    assert("2020-06-16" == log_entry_date)

    val event_type = ruckusSyslogSource.message.split(",")(1)
    assert("clientJoin" == event_type)

    assert("172.26.147.234" == ruckusMessageSource.apIpAddress)

    val ruckusSyslog = RuckusSyslog(
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
        first_auth = toLong(ruckusMessageSource.firstAuth),
        association_time = toLong(ruckusMessageSource.associationTime),
        ip_assign_time = toLong(ruckusMessageSource.ipAssignTime),
        disconnect_time = toLong(ruckusMessageSource.disconnectTime),
        session_duration = toLong(ruckusMessageSource.sessionDuration),
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
    
    assert("04BA8D6CC5CD" == ruckusSyslog.client_mac)
    assert(109 == ruckusSyslog.vlan)
    assert(2887422954L == ruckusSyslog.ap_ip)
  }  


    test("RuckusSyslog test 20200710") {
        val deserializer = new RuckusSyslogDeserializer(null)
        val p = deserializer.parseJson(payload_20200710)
        assert(p.nonEmpty)
        assert(Option.empty == p.get.ip_assign_time)
    }  
}

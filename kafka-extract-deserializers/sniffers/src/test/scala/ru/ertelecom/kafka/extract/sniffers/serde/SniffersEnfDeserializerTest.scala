package ru.ertelecom.kafka.extract.sniffers.serde
import java.nio.charset.StandardCharsets
import org.scalatest.FunSuite
class SniffersEnfDeserializerTest  extends FunSuite {
  test("successSniffersEnfDeserializerTest") {
    val payload ="""
[{"Channel": "9", "FramesCount": "3", "KnownFrom": "1571220996", "LastSeen": "1571247397", "MAC": "80:00:0b:2c:1c:0d", "NotifiedCount": "357", "RSSI": "-82", "RSSImax": "-81", "SnifID": "e48d8cd5de27", "SnifIP": "10.77.139.240", "Vendor": "Intel Corporate"},
 {"Channel": "2", "FramesCount": "4", "KnownFrom": "1571247396", "LastSeen": "1571247396", "MAC": "90:61:ae:f8:df:9f", "NotifiedCount": "2", "RSSI": "-51", "RSSImax": "-51", "SnifID": "6c3b6b931fed", "SnifIP": "10.147.99.18", "Vendor": "Intel Corporate"},
  {"Channel": "13", "FramesCount": "3", "KnownFrom": "1571203721", "LastSeen": "1571247401", "MAC": "70:c9:4e:4a:68:74", "NotifiedCount": "2319", "RSSI": "-90", "RSSImax": "-90", "SnifID": "744d28a00ba3", "SnifIP": "10.76.99.250", "Vendor": "Liteon Technology Corporation"},
  {"Channel": "13", "FramesCount": "4", "KnownFrom": "1571246007", "LastSeen": "1571247398", "MAC": "30:57:14:31:8a:62", "NotifiedCount": "36", "RSSI": "-83", "RSSImax": "-82", "SnifID": "744d28a00ba3", "SnifIP": "10.76.99.250", "Vendor": "Apple, Inc."},
  {"Channel": "2", "FramesCount": "1", "KnownFrom": "1571247398", "LastSeen": "1571247398", "MAC": "94:db:c9:4b:e6:d8", "NotifiedCount": "1", "RSSI": "-90", "RSSImax": "-90", "SnifID": "6c3b6b931fed", "SnifIP": "10.147.99.18", "Vendor": "AzureWave Technology Inc."},
  {"Channel": "2", "FramesCount": "1", "KnownFrom": "1571049710", "LastSeen": "1571247401", "MAC": "30:ae:a4:56:e7:04", "NotifiedCount": "26964", "RSSI": "-86", "RSSImax": "-86", "SnifID": "6c3b6b931fed", "SnifIP": "10.147.99.106", "Vendor": "Espressif Inc."},
  {"Channel": "13", "FramesCount": "2", "KnownFrom": "1571246762", "LastSeen": "1571247398", "MAC": "54:25:ea:8a:8b:8d", "NotifiedCount": "40", "RSSI": "-84", "RSSImax": "-84", "SnifID": "744d28a00ba3", "SnifIP": "10.76.99.250", "Vendor": "HUAWEI TECHNOLOGIES CO.,LTD"}]
    """.stripMargin.getBytes
    val deserializer = new SniffersEnfDeserializer(null)
    val sniffersEnfOption = deserializer.parseJson(payload)
    assert(deserializer.parseJson(payload).isDefined)
    val SniffersEnf = sniffersEnfOption.get
    assert(1571247396 == SniffersEnf(1).ts)
   assert("6C3B6B931FED" == SniffersEnf(1).sensor)
    assert("9061AEF8DF9F" == SniffersEnf(1).mac)
    assert(177431314 == SniffersEnf(1).snif_ip)
  }
}

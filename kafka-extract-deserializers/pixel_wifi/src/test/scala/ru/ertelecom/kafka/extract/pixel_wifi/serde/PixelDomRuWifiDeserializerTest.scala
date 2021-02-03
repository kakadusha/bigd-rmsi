package ru.ertelecom.kafka.extract.pixel_wifi.test

import java.nio.charset.Charset

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.pixel_wifi.serde.PixelDomRuWifiDeserializer


class PixelDomRuWifiDeserializerTest extends FunSuite {

  test("success PixelDomRuWifiDeserializer on FullPacket") {
    val deserializer = new PixelDomRuWifiDeserializer(null)
    val testValue = """2019-12-04T14:16:56+00:00	91.144.154          .5	/wifi.gif?dzIxTTMRbl0NDAJhYQhLQRJRCnUBZBZ8TTZjVwgScjQkUBkQGjRzBQZ/YwtURGRdYAYHYnR9HwM6Byt5VzJvC0pAZwIvVlQ3YQpOUx8LLVVCOD1XMxFuVXcBA39hD0tEdTQvU1MYNgRfRxUUIUZCMD4fKBomECtGfzVvWBYWMhArWBAFO1QfBicFI0QLYGcOT0FlXX8HBQ==	Mozilla/5.0 (Linux; arm_64; Android 9; LLD-L31) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.92 YaBrowser/19.10.1.100.00 Mobile Safari/537.36	uid=		p_uid=589CBABCA6F0BE5D034EF8AC026A2D1B	http_ref=https://chelny.wifi.domru.ru/portal	"""
    val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))

    assert(p.isDefined)
    assert(p.get.loginid.isEmpty)
    assert(p.get.acctid == Option.apply("9C8403114A5DA75DE7C03F"))
    assert(p.get.deviceip == Option.apply("10.12.179.213"))
    assert(p.get.devicemac == Option.apply("2054fabbf334"))
    assert(p.get.locationid == Option.apply("1955.3611"))
    assert(p.get.pageid == "/portal")
    assert(p.get.routerid == Option.apply("alcatel"))
    assert(p.get.clientts == 1575469133000L)
    assert(p.get.datehourminute == 1575469016000L)
    assert(p.get.client_id.isEmpty)
    assert(p.get.ua == "Mozilla/5.0 (Linux; arm_64; Android 9; LLD-L31) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.92 YaBrowser/19.10.1.100.00 Mobile Safari/537.36")
    assert(p.get.ip == "91.144.154.5")
    assert(p.get.puid == Option.apply("589CBABCA6F0BE5D034EF8AC026A2D1B"))
    assert(p.get.http_ref == Option.apply("https://chelny.wifi.domru.ru/portal"))
    assert(p.get.citydomain.isEmpty)
    assert(p.get.userid.isEmpty)
    assert(p.get.vkid.isEmpty)
    assert(p.get.okid.isEmpty)
    assert(p.get.fbid.isEmpty)
    assert(p.get.twid.isEmpty)
    assert(p.get.instid.isEmpty)
    assert(p.get.cityid.isEmpty)
    assert(p.get.browser_name == Option.apply("Yandex Browser"))
    assert(p.get.browser_version == Option.apply("19.10.1"))
    assert(p.get.os_name == Option.apply("Android"))
    assert(p.get.os_version == Option.apply("9"))
    assert(p.get.device == Option.apply("Huawei LLD-L31"))
    assert(p.get.year == 2019)
    assert(p.get.month == 12)

  }

  test("test CheckFailOnStatsPacket") {
    val deserializer = new PixelDomRuWifiDeserializer(null)
    val testValue = """2019-11-19T15:45:07+00:00	176.215.82.85	/stat.gif?v=1&_v=j79&a=445906170&t=pageview&_s=1&dl=http%3A%2F%2Fst.domru.ru%2F%3F_ga%3D2.107690050.115794187.1574133970-877202006.1573467768&ul=ru-ru&de=UTF-8&dt=%D0%9F%D1%80%D0%BE%D0%B2%D0%B5%D1%80%D0%BA%D0%B0%20%D1%81%D0%BA%D0%BE%D1%80%D0%BE%D1%81%D1%82%D0%B8%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%D0%B0%2C%20%D1%82%D0%B5%D1%81%D1%82%20%D1%81%D0%BA%D0%BE%D1%80%D0%BE%D1%81%D1%82%D0%B8%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82-%D1%81%D0%BE%D0%B5%D0%B4%D0%B8%D0%BD%D0%B5%D0%BD%D0%B8%D1%8F&sd=24-bit&sr=412x846&vp=412x718&je=0&_u=yCEAiEABh~&jid=&gjid=&cid=877202006.1573467768&uid=240009311082&tid=UA-42895529-1&_gid=115794187.1574133970&gtm=2wgav3M43KXLC&cd5=&cd6=240009311082&cd7=877202006.1573467768&z=1710393167	Mozilla/5.0 (Linux; Android 9; SM-A920F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.96 Mobile Safari/537.36	uid=240009311082		p_uid=589CBABCBEB8525DB529EB6A029BE2F9	http_ref=http://st.domru.ru/?_ga=2.107690050.115794187.1574133970-877202006.1573467768	krsk"""
    val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
    assert(p.isEmpty)
  }

   /* keep this test the last one, this makes possible to turn off try-catch block and get exception on exact error line */
   test("test EmptyTsv") {
     val deserializer = new PixelDomRuWifiDeserializer(null)
     val testValue = ""
     val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
     assert(p.isEmpty)
   }

}

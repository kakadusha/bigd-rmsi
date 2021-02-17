package ru.ertelecom.kafka.extract.clickstream.serde

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.clickstream.domain.Clickstream

class ClickstreamDeserializerTest extends FunSuite{

  test("parsing all not empty") {
    val inMsg = "20502\tnnov1-soft\t1561384328\t192.168.0.1\ttest.test\t" +
      "Mozilla/5.0 (BB10; Touch) AppleWebKit/537.3+ (KHTML, like Gecko) Version/10.0.9.388 Mobile Safari/537.3+\t" +
      "test_login\trambler_ruid=test0;yandex_uid=test1;1dmp=test2;weborama_affice=weborama_test\tb4:51:f9:d5:a9:0c"
    val expectedClickstream = Clickstream(
      timestamp_local = 1561384328000L,
      timestamp_utc = 1561373528000L,
      login = "test_login",
      ip = "192.168.0.1",
      url = "test.test",
      ua = "Mozilla/5.0 (BB10; Touch) AppleWebKit/537.3+ (KHTML, like Gecko) Version/10.0.9.388 Mobile Safari/537.3+",
      city_id = 136,
      date_local = "2019-06-24",
      cookie1 = Option.apply("test0"),
      cookie2 = Option.apply("test1"),
      cookie3 = Option.apply("weborama_test"),
      browser_name = Option.apply("BlackBerry WebKit"),
      browser_version = Option.apply("10.0.9"),
      os_name = Option.apply("BlackBerry OS"),
      os_version = Option.apply("10.0.9"),
      device = Option.apply("BlackBerry Touch"),
      mac = Option.apply("B451F9D5A90C"),
      router_manufacturer = Option.apply("NB")
    )
    val deserializer = new ClickstreamDeserializer(null)
    val actualClickstream = deserializer.parseTsv(inMsg)
    assert(actualClickstream.isDefined)
    assert(expectedClickstream == actualClickstream.get)
  }

  test("all options is empty") {
    val inMsg = "20502\tnnov1-soft\t1561384328\t192.168.0.1\ttest.test\t\ttest_login\t1dmp=test2\t"
    val expectedClickstream = Clickstream(
      timestamp_local = 1561384328000L,
      timestamp_utc = 1561373528000L,
      login = "test_login",
      ip = "192.168.0.1",
      url = "test.test",
      ua = "",
      city_id = 136,
      date_local = "2019-06-24",
      cookie1 = Option.empty,
      cookie2 = Option.empty,
      cookie3 = Option.empty,
      browser_name = Option.empty,
      browser_version = Option.empty,
      os_name = Option.empty,
      os_version = Option.empty,
      device = Option.empty,
      mac = Option.empty,
      router_manufacturer = Option.empty
    )
    val deserializer = new ClickstreamDeserializer(null)
    val actualClickstream = deserializer.parseTsv(inMsg)
    assert(actualClickstream.isDefined)
    assert(actualClickstream.get == expectedClickstream)
  }

}

package ru.ertelecom.kafka.extract.ac_ad_equila.serde

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.ac_ad_equila.domain.AccessLogAdEquila

class AccessLogAdEquilaDeserializerTest extends FunSuite {
  test("test-s-show-action") {
    val tsv = "s\t2020-04-08T01:34:12+02:00\t188.233.77.174\t/js/2.js?campId=1297746&u=A75ACBBF89823727&bid=215\t" +
      "Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Mobile/15E148 Safari/604.1\t" +
      "uid=-\tp_uid=589CBABC4AFD8D5E870594B002BCC50E\t-\thttp_ref=http://www.hdseason.ru/\t-\tA75ACBBF89823727\t215\t1297746"

    val deserializer = new AccessLogAdEquilaDeserializer(null)
    val actual = deserializer.parseTsv(tsv).get

    val expected = AccessLogAdEquila(
      action = "s",
      ts = 1586302452000L,
      ip = 3169406382L,
      req_uri = "/js/2.js?campId=1297746&u=A75ACBBF89823727&bid=215",
      ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 12_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Mobile/15E148 Safari/604.1",
      uid = None,
      puid = Option("589CBABC4AFD8D5E870594B002BCC50E"),
      ref = Option("http://www.hdseason.ru/"),
      city_id = None,
      user_id = Option("A75ACBBF89823727"),
      billing_id = Option(215),
      campaign_id = Option(1297746L),
      login = Option("v1185803"),
      for_day = "2020-04-07"
    )
    assert(actual == expected)
  }

  test("test-a-click-action") {
    val tsv = "a\t2020-04-08T23:34:12+00:00\t5.167.158.9\t" +
      "/go?url=https%3A%2F%2Fdomru.ru%2Fdomru-tv%2Fequipment%2Fmovix_pro%3Futm_source%3Dban" +
      "ner%26utm_medium%3DTargetMarketing%26utm_campaign%3Ddomavozmozhn\n" +
      "o%26utm_term%3Dbutton5%26u%3D11EBA2F8AB7F16D236ED84B2B875EBD11BABC6C992A5DF0C%26b" +
      "id%3D552%26campId%3D1297749&u=11EBA2F8AB7F16D236ED84B2B875EBD11BABC6C992A5DF0C&bid=552&campId=1297749\t" +
      "Mozilla/5.0 (iPhone; CPU iPh\n" +
      "one OS 10_3_4 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gec" +
      "ko) Version/10.0 Mobile/14G61 Safari/602.1\t" +
      "uid=-\t" +
      "p_uid=D78AEABCDD048F5E780C4C77022B0303\t" +
      "-\t" +
      "http_ref=http://display.360totalsecurity.com/inapp/bootup_tray?cid=101&lang=ru&mid=af87cfcaf447531987d0549a22decb6f&sch=0&utm_content=SB.banner&utm_medium=banner&utm_source=IA&db=Google%20Chrome&o=0&t=1586798330&dailynews=0\t" +
      "-\t" +
      "11EB\nA2F8AB7F16D236ED84B2B875EBD11BABC6C992A5DF0C" +
      "\t552" +
      "\t1297749"

    val deserializer = new AccessLogAdEquilaDeserializer(null)
    val actual = deserializer.parseTsv(tsv).get

    val expected = AccessLogAdEquila(
      action = "a",
      ts = 1586388852000L,
      ip = 94871049L,
      req_uri = "/go?url=https%3A%2F%2Fdomru.ru%2Fdomru-tv%2Fequipment%2Fmovix_pro%3Futm_source%3Dbanner%26utm_medium%3DTargetMarketing%26utm_campaign%3Ddomavozmozhno%26utm_term%3Dbutton5%26u%3D11EBA2F8AB7F16D236ED84B2B875EBD11BABC6C992A5DF0C%26bid%3D552%26campId%3D1297749&u=11EBA2F8AB7F16D236ED84B2B875EBD11BABC6C992A5DF0C&bid=552&campId=1297749",
      ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_4 like Mac OS X) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.0 Mobile/14G61 Safari/602.1",
      uid = None,
      puid = Option("D78AEABCDD048F5E780C4C77022B0303"),
      ref = Option("http://display.360totalsecurity.com/inapp/bootup_tray?cid=101&lang=ru&mid=af87cfcaf447531987d0549a22decb6f&sch=0&utm_content=SB.banner&utm_medium=banner&utm_source=IA&db=Google%20Chrome&o=0&t=1586798330&dailynews=0"),
      city_id = None,
      user_id = Option("11EBA2F8AB7F16D236ED84B2B875EBD11BABC6C992A5DF0C"),
      billing_id = Option(552),
      campaign_id = Option(1297749L),
      login = Option("89179862396@sochi20"),
      for_day = "2020-04-08"
    )
    assert(actual == expected)
  }
}

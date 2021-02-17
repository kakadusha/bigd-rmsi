package ru.ertelecom.kafka.extract.ac_ad_equila_s.serde

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.ac_ad_equila_s.domain.AccessLogAdEquilaS

class AccessLogAdEquilaSDeserializerTest extends FunSuite {

  test("test-date-before-00") {
    val tsv =
      "2020-04-08T01:34:12+02:00" +
      "#0115.166.52.54" +
      "#011/info/link?u=http%3A%2F%2Finfo.ertelecom.ru%2F%3FcampId%3" +
        "D26689%26machine%3Dekat%26u%3D88347F59B486E38268B26B997DFEF8FF1851594FEB3700DD&ts=1586969024&sid=3c3d90fb9" +
        "584e59dd41bb157818001af&ourl=http%3A%2F%2Fkino-driver.ru%2Frikoshet-13-14-seriya-na-ntv-2341778%2F" +
      "#011Mozilla/5.0 (iPhone; CPU iPhone OS 13_3_1 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) GSA/49.0.195" +
        "456936 Mobile/17D50 Safari/604.1" +
//      "#011uid=590011531595" +
      "#011uid=-" +
      "#011-#011-" +
      "#011http_ref=http://kino-driver.ru/rikoshet-13-14-seriya-na-ntv-2341778/" +
      "#011-" +
      "#011http%3A%2F%2Finfo.ertelecom.ru%2F%3FcampId%3D26689%26machine%3Dekat%26u%3D88347F59B4" +
        "86E38268B26B997DFEF8FF1851594FEB3700DD" +
      "#011http%3A%2F%2Fkino-driver.ru%2Frikoshet-13-14-seriya-na-ntv-2341778%2F" +
      "#011http://info.ertelecom.ru/?campId=26689&machine=ekat&u=88347F59B486E38268B26B997DFEF8FF1851594FEB3" +
        "700DD&ourl=http%3A%2F%2Fkino-driver.ru%2Frikoshet-13-14-seriya-na-ntv-2341778%2F&timestamp$c=158696" +
        "8966&sid$c=d529eec3e99ffbe756c3ddf6170325cb"

    val deserializer = new AccessLogAdEquilaSDeserializer(null)
    val actual = deserializer.parseTsv(tsv).get

    val expected = AccessLogAdEquilaS(
      ts = 1586302452000L,
      ip = 94778422L,
//      req_uri = "/info/link?u=http%3A%2F%2Finfo.ertelecom.ru%2F%3FcampId%3D26689%26machine%3Dekat%26u%3D88347F59B486E38268B26B997DFEF8FF1851594FEB3700DD&ts=1586969024&sid=3c3d90fb9584e59dd41bb157818001af&ourl=http%3A%2F%2Fkino-driver.ru%2Frikoshet-13-14-seriya-na-ntv-2341778%2F",
      campaign_id = Option(26689L),
      machine = Option("ekat"),
      sid = Option("3c3d90fb9584e59dd41bb157818001af"),
      ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 13_3_1 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) GSA/49.0.195456936 Mobile/17D50 Safari/604.1",
      uid = None,
      puid = None,
      ref = Option("http://kino-driver.ru/rikoshet-13-14-seriya-na-ntv-2341778/"),
      city_id = None,
      user_id = Option("88347F59B486E38268B26B997DFEF8FF1851594FEB3700DD"),
      ourl = Option("http%3A%2F%2Fkino-driver.ru%2Frikoshet-13-14-seriya-na-ntv-2341778%2F"),
      http_location = Option("http://info.ertelecom.ru/?campId=26689&machine=ekat&u=88347F59B486E38268B26B997DFEF8FF1851594FEB3700DD&ourl=http%3A%2F%2Fkino-driver.ru%2Frikoshet-13-14-seriya-na-ntv-2341778%2F&timestamp$c=1586968966&sid$c=d529eec3e99ffbe756c3ddf6170325cb"),
      login = Some("89221420435@sochi20"),
      for_day = "2020-04-07"
    )
    assert(actual == expected)
  }

}

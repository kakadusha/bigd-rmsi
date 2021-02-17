package ru.ertelecom.dpi.eql.serde

import org.scalatest.FunSuite
import ru.ertelecom.dpi.eql.domain.EquilaDpiCampaignLog

class EquilaDpiCampaignDeserializerTest extends FunSuite{
  test("test redirect") {
    val tsv = "1100\t2\t1583884200\tv41617618\t6011967746154151013\t192.168.10.10\t121.55.55.55\t" +
      "neochemical.ru/SiteNN/js/jquery.cookie.js\t" +
      "Mozilla/5.0 (iPhone; CPU iPhone OS 13_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1\t" +
      "1297740\t0\t000000002f312e3120333032204d6f76\thttp://neochemical.ru/\t6"

    val deserializer = new EquilaDpiCampaignDeserializer(null)
    val actual = deserializer.parseTsv(tsv).get

    val expected = EquilaDpiCampaignLog(
      billing_id = 1100,
      box_id = 2,
      ts = 1583884200000L,
      login = "v41617618",
      device_id = "6011967746154151013",
      client_private_ip = 3232238090L,
      client_request_ip = 2033661751L,
      url = "neochemical.ru/SiteNN/js/jquery.cookie.js",
      ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 13_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1",
      campaign_id = 1297740,
      event_type = 0,
      sid = "000000002f312e3120333032204d6f76",
      referer = "http://neochemical.ru/",
      msgType = Option(6),
      for_day = "2020-03-10"
    )

    assert(actual == expected)
  }

  test("test no msgType and referer and event_type = 0") {
    val tsv = "1100\t2\t1583884200\tv41617618\t6011967746154151013\t192.168.10.10\t121.55.55.55\t" +
      "neochemical.ru/SiteNN/js/jquery.cookie.js\t" +
      "Mozilla/5.0 (iPhone; CPU iPhone OS 13_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1\t" +
      "1297740\t0\t000000002f312e3120333032204d6f76"

    val deserializer = new EquilaDpiCampaignDeserializer(null)
    val actual = deserializer.parseTsv(tsv).get

    val expected = EquilaDpiCampaignLog(
      billing_id = 1100,
      box_id = 2,
      ts = 1583884200000L,
      login = "v41617618",
      device_id = "6011967746154151013",
      client_private_ip = 3232238090L,
      client_request_ip = 2033661751L,
      url = "neochemical.ru/SiteNN/js/jquery.cookie.js",
      ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 13_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1",
      campaign_id = 1297740,
      event_type = 0,
      sid = "000000002f312e3120333032204d6f76",
      referer = null,
      msgType = Option(0),
      for_day = "2020-03-10"
    )

    assert(actual == expected)
  }

  test("test no msgType and referer and event_type != 0") {
    val tsv = "1100\t2\t1583884200\tv41617618\t6011967746154151013\t192.168.10.10\t121.55.55.55\t" +
      "neochemical.ru/SiteNN/js/jquery.cookie.js\t" +
      "Mozilla/5.0 (iPhone; CPU iPhone OS 13_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1\t" +
      "1297740\t2\t000000002f312e3120333032204d6f76"

    val deserializer = new EquilaDpiCampaignDeserializer(null)
    val actual = deserializer.parseTsv(tsv).get

    val expected = EquilaDpiCampaignLog(
      billing_id = 1100,
      box_id = 2,
      ts = 1583884200000L,
      login = "v41617618",
      device_id = "6011967746154151013",
      client_private_ip = 3232238090L,
      client_request_ip = 2033661751L,
      url = "neochemical.ru/SiteNN/js/jquery.cookie.js",
      ua = "Mozilla/5.0 (iPhone; CPU iPhone OS 13_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1",
      campaign_id = 1297740,
      event_type = 2,
      sid = "000000002f312e3120333032204d6f76",
      referer = null,
      msgType = Option(100),
      for_day = "2020-03-10"
    )

    assert(actual == expected)
  }
}

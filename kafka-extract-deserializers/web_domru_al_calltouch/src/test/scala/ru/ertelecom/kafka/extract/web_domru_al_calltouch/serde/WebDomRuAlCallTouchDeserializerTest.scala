package ru.ertelecom.kafka.extract.web_domru_al_calltouch.serde

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.web_domru_al_calltouch.domain.WebDomRuAlCallTouch

class WebDomRuAlCallTouchDeserializerTest extends FunSuite {

  test("test-date-before-00") {
    val tsv = " 2020-05-19T01:34:12+02:00\t" +
      "92.118.67.252\t" +
      "/ct.gif?callphase=calldisconnected&callerphone=79110089191&phonenumber=78122104159" +
      "&redirectNumber=sip%3A78124547447%40188.187.255.25&duration=199&waiting_time=0&timestamp=" +
      "&source=google&medium=cpc&utm_source=%3C%D0%BD%D0%B5+%D0%B7%D0%B0%D0%BF%D0%BE%D0%BB%D0%BD%D0%B5%D0%BD%D0%BE%3E" +
      "&utm_medium=%3C%D0%BD%D0%B5+%D0%B7%D0%B0%D0%BF%D0%BE%D0%BB%D0%BD%D0%B5%D0%BD%D0%BE%3E" +
      "&utm_campaign=%D0%91%D1%80%D0%B5%D0%BD%D0%B4+%D0%9F%D0%BE%D0%B8%D1%81%D0%BA" +
      "&utm_term=%3C%D0%BD%D0%B5+%D0%B7%D0%B0%D0%BF%D0%BE%D0%BB%D0%BD%D0%B5%D0%BD%D0%BE%3E" +
      "&sessionId=0&hostname=\t79110089191\t78122104159\t199\t-\t-\tgoogle\t\t-\t-"
    
    val actual = new WebDomRuAlCallTouchDeserializer(null).parseTsv(tsv).get
    val expected = WebDomRuAlCallTouch(
      ts = 1589844852000L,
      ip = 1551254524L,
      callphase = Option("calldisconnected"),
      callerphone = Option("79110089191"),
      phonenumber = Option("78122104159"),
      redirectNumber = Option("sip:78124547447@188.187.255.25"),
      source = Option("google"),
      medium = Option("cpc"),
      utm_source = Option("<не заполнено>"),
      utm_medium = Option("<не заполнено>"),
      utm_campaign = Option("Бренд Поиск"),
      utm_term = Option("<не заполнено>"),
      gcid = None,
      yaClientId = None,
      sessionId = Some("0"),
      hostname = None,
      ourl = None,
      referer = None,
      user_agent = None,
      for_day = "2020-05-18"
    )

    assert(actual == expected)
  }

  test("test-date-after-00") {
    val tsv = " 2020-05-19T04:51:07+00:00\t" +
      "92.118.67.252\t" +
      "/ct.gif?callphase=callconnected&callerphone=79531548441&phonenumber=78124248798" +
      "&redirectNumber=undefined&timestamp=1589863865&source=google&medium=organic" +
      "&utm_source=%3C%D0%BD%D0%B5+%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D0%BD%D0%BE%3E" +
      "&utm_medium=%3C%D0%BD%D0%B5+%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D0%BD%D0%BE%3E" +
      "&utm_campaign=%3C%D0%BD%D0%B5+%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D0%BD%D0%BE%3E" +
      "&utm_term=%3C%D0%BD%D0%B5+%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D0%BD%D0%BE%3E" +
      "&gcid=524613580.1589863856&yaClientId=1589863855341277874&sessionId=681224962" +
      "&hostname=interzet.domru.ru&url=https%3A%2F%2Finterzet.domru.ru%2F" +
      "&ref=https%3A%2F%2Fwww.google.ru%2F" +
      "&userAgent=Mozilla%2F5.0+%28iPhone%3B+CPU+iPhone+OS+13_3_1+like+Mac+OS+X%29+AppleWebKit" +
      "%2F605.1.15+%28KHTML%2C+like+Gecko%29+Version%2F13.0.5+Mobile%2F15E148+Safari%2F604.1\t" +
      "79531548441\t78124248798\t-\t524613580.1589863856\t1589863855341277874\tgoogle\t" +
      "interzet.domru.ru\thttps%3A%2F%2Finterzet.domru.ru%2F\t" +
      "Mozilla%2F5.0+%28iPhone%3B+CPU+iPhone+OS+13_3_1+like+Mac+OS+X%29+AppleWebKit%2F605.1.15+%28KHTML%2C+" +
      "like+Gecko%29+Version%2F13.0.5+Mobile%2F15E148+Safari%2F604.1"

    val actual = new WebDomRuAlCallTouchDeserializer(null).parseTsv(tsv).get
    val expected = WebDomRuAlCallTouch(
      ts = 1589863865000L,
      ip = 1551254524L,
      callphase = Option("callconnected"),
      callerphone = Option("79531548441"),
      phonenumber = Option("78124248798"),
      redirectNumber = Option("undefined"),
      source = Option("google"),
      medium = Option("organic"),
      utm_source = Option("<не указано>"),
      utm_medium = Option("<не указано>"),
      utm_campaign = Option("<не указано>"),
      utm_term = Option("<не указано>"),
      gcid = Option("524613580.1589863856"),
      yaClientId = Option("1589863855341277874"),
      sessionId = Some("681224962"),
      hostname = Option("interzet.domru.ru"),
      ourl = Option("https://interzet.domru.ru/"),
      referer = Option("https://www.google.ru/"),
      user_agent = Option("Mozilla/5.0 (iPhone; CPU iPhone OS 13_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Mobile/15E148 Safari/604.1"),
      for_day = "2020-05-19"
    )

    assert(actual == expected)
  }

  test("test-date-after-all") {

    val tsv = "2020-05-20T13:32:18+00:00\t92.118.67.252\t" +
      "/ct.gif?callphase=callconnected" +
      "&callerphone=79650070682&phonenumber=78124456047" +
      "&redirectNumber=undefined" +
      "&timestamp=1589981536" +
      "&source=perm.domru.ru&medium=referral" +
      "&utm_source=%3C%D0%BD%D0%B5+%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D0%BD%D0%BE%3E" +
      "&utm_medium=%3C%D0%BD%D0%B5+%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D0%BD%D0%BE%3E" +
      "&utm_campaign=%3C%D0%BD%D0%B5+%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D0%BD%D0%BE%3E" +
      "&utm_term=%3C%D0%BD%D0%B5+%D1%83%D0%BA%D0%B0%D0%B7%D0%B0%D0%BD%D0%BE%3E" +
      "&gcid=349216031.1589981380&yaClientId=158998138099370501&sessionId=682331683" +
      "&hostname=interzet.domru.ru" +
      "&url=https%3A%2F%2Finterzet.domru.ru%2Fpayments%3Fagreement%3D780032895580%26clientId%3DCAMPAIGN%26timestamp" +
      "%3D20200520013147%26secret%3Dc521622d80bf8272d840f6513057c564%26grantType%3Dreferal%26city_id%3D556%26ctrk%3D12418269003" +
      "&ref=https%3A%2F%2Fperm.domru.ru%2Fpayments%3Fagreement%3D780032895580%26clientId%3DCAMPAIGN%26timestamp" +
      "%3D20200520013147%26secret%3Dc521622d80bf8272d840f6513057c564%26grantType%3Dreferal%26city_id%3D556%26ctrk%3D12418269003" +
      "&userAgent=Mozilla%2F5.0+%28Linux%3B+Android+9%3B+SAMSUNG+SM-G975F%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+" +
      "SamsungBrowser%2F11.2+Chrome%2F75.0.3770.143+Mobile+Safari%2F537.36\t79650070682\t78124456047\t-\t" +
      "349216031.1589981380\t158998138099370501\tperm.domru.ru\tinterzet.domru.ru\t" +
      "https%3A%2F%2Finterzet.domru.ru%2Fpayments%3Fagreement%3D780032895580%26clientId%3DCAMPAIGN%26timestamp%3D20200520013147" +
      "%26secret%3Dc521622d80bf8272d840f6513057c564%26grantType%3Dreferal%26city_id%3D556%26ctrk%3D12418269003\t" +
      "Mozilla%2F5.0+%28Linux%3B+Android+9%3B+SAMSUNG+SM-G975F%29+AppleWebKit%2F537.36+%28KHTML%2C+like+Gecko%29+" +
      "SamsungBrowser%2F11.2+Chrome%2F75.0.3770.143+Mobile+Safari%2F537.36"


    val actual = new WebDomRuAlCallTouchDeserializer(null).parseTsv(tsv).get
    val expected = WebDomRuAlCallTouch(
      ts = 1589981536000L,
      ip = 1551254524L,
      callphase = Option("callconnected"),
      callerphone = Option("79650070682"),
      phonenumber = Option("78124456047"),
      redirectNumber = Option("undefined"),
      source = Option("perm.domru.ru"),
      medium = Option("referral"),
      utm_source = Option("<не указано>"),
      utm_medium = Option("<не указано>"),
      utm_campaign = Option("<не указано>"),
      utm_term = Option("<не указано>"),
      gcid = Option("349216031.1589981380"),
      yaClientId = Option("158998138099370501"),
      sessionId = Some("682331683"),
      hostname = Option("interzet.domru.ru"),
      ourl = Option("https://interzet.domru.ru/payments?agreement=780032895580&clientId=CAMPAIGN&timestamp=20200520013147&secret=c521622d80bf8272d840f6513057c564&grantType=referal&city_id=556&ctrk=12418269003"),
      referer = Option("https://perm.domru.ru/payments?agreement=780032895580&clientId=CAMPAIGN&timestamp=20200520013147&secret=c521622d80bf8272d840f6513057c564&grantType=referal&city_id=556&ctrk=12418269003"),
      user_agent = Option("Mozilla/5.0 (Linux; Android 9; SAMSUNG SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/11.2 Chrome/75.0.3770.143 Mobile Safari/537.36"),
      for_day = "2020-05-20"
    )

    assert(actual == expected)
  }

}

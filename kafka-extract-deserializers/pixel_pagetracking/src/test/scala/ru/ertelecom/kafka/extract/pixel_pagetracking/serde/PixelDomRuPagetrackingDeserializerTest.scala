package ru.ertelecom.kafka.extract.pixel_pagetracking.serde

import java.nio.charset.Charset
import org.scalatest.FunSuite

class PixelDomRuPagetrackingDeserializerTest extends FunSuite {

  test("successPixelDomRuPagetrackingDeserializer/FullPacket") {
    val deserializer = new PixelDomRuPagetrackingDeserializer(null)
    val testValue = """2019-11-19T15:45:07+00:00	176.215.82.85	/stat.gif?v=1&_v=j79&a=445906170&t=pageview&_s=1&dl=http%3A%2F%2Fst.domru.ru%2F%3F_ga%3D2.107690050.115794187.1574133970-877202006.1573467768&ul=ru-ru&de=UTF-8&dt=%D0%9F%D1%80%D0%BE%D0%B2%D0%B5%D1%80%D0%BA%D0%B0%20%D1%81%D0%BA%D0%BE%D1%80%D0%BE%D1%81%D1%82%D0%B8%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%D0%B0%2C%20%D1%82%D0%B5%D1%81%D1%82%20%D1%81%D0%BA%D0%BE%D1%80%D0%BE%D1%81%D1%82%D0%B8%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82-%D1%81%D0%BE%D0%B5%D0%B4%D0%B8%D0%BD%D0%B5%D0%BD%D0%B8%D1%8F&sd=24-bit&sr=412x846&vp=412x718&je=0&_u=yCEAiEABh~&jid=&gjid=&cid=877202006.1573467768&uid=240009311082&tid=UA-42895529-1&_gid=115794187.1574133970&gtm=2wgav3M43KXLC&cd5=&cd6=240009311082&cd7=877202006.1573467768&cd8=%D1%82%D0%B5%D1%81%D1%82%D0%BE%D0%B2%D1%8B%D0%B9%20%D1%82%D0%B5%D1%81%D1%82&cd9=%2B7(989)898-77-76&z=1710393167	Mozilla/5.0 (Linux; Android 9; SM-A920F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.96 Mobile Safari/537.36	uid=240009311082		p_uid=589CBABCBEB8525DB529EB6A029BE2F9	http_ref=http://st.domru.ru/?_ga=2.107690050.115794187.1574133970-877202006.1573467768	krsk"""
    val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))

    assert(p.isDefined)
    assert(p.get.log_entry_date == "2019-11-19")
    assert(p.get.ts == 1574178307)
    assert(p.get.client_id == Option.apply("240009311082"))
    assert(p.get.javaenabled == "0")
    assert(p.get.flashversion.isEmpty)
    assert(p.get.lang == Option.apply("ru-ru"))
    assert(p.get.screencolors == Option.apply("24-bit"))
    assert(p.get.screenresolution == Option.apply("412x846"))
    assert(p.get.windowresolution == Option.apply("412x718"))
    assert(p.get.codepage == Option.apply("UTF-8"))
    assert(p.get.ua == "Mozilla/5.0 (Linux; Android 9; SM-A920F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.96 Mobile Safari/537.36")
    assert(p.get.ip == 2966901333L)
    assert(p.get.url == "http://st.domru.ru/?_ga=2.107690050.115794187.1574133970-877202006.1573467768")
    assert(p.get.hostname.isEmpty)
    assert(p.get.pathtopage.isEmpty)
    assert(p.get.screenview.isEmpty)
    assert(p.get.title == Option.apply("Проверка скорости интернета, тест скорости интернет-соединения"))
    assert(p.get.web_id == Option.apply("UA-42895529-1"))
    assert(p.get.user_id == Option.apply("877202006.1573467768"))
    assert(p.get.ref.isEmpty)
    assert(p.get.hit_type == Option.apply("pageview"))
    assert(p.get.pageloadtime.isEmpty)
    assert(p.get.pagedownloadtime.isEmpty)
    assert(p.get.redirectresponcetime.isEmpty)
    assert(p.get.serverresponcetime.isEmpty)
    assert(p.get.contentloadtime.isEmpty)
    assert(p.get.event_category.isEmpty)
    assert(p.get.event_action.isEmpty)
    assert(p.get.event_label.isEmpty)
    assert(p.get.event_value.isEmpty)
    assert(p.get.p_uid == Option.apply("589CBABCBEB8525DB529EB6A029BE2F9"))
    assert(p.get.http_ref == Option.apply("http://st.domru.ru/?_ga=2.107690050.115794187.1574133970-877202006.1573467768"))
    assert(p.get.gtm == Option.apply("M43KXLC"))
    assert(p.get.citydomain == Option.apply("krsk"))
    assert(p.get.browser_name == Option.apply("Chrome Mobile"))
    assert(p.get.browser_version == Option.apply("78.0.3904"))
    assert(p.get.os_name == Option.apply("Android"))
    assert(p.get.os_version == Option.apply("9"))
    assert(p.get.device == Option.apply("Samsung SM-A920F"))
  }

  test("test 20191128 failed package on v05") {
    val deserializer = new PixelDomRuPagetrackingDeserializer(null)
    val testValue = """2019-11-18T08:54:29+00:00	188.233.239.208	/stat.gif?v=1&_v=j79&a=1063434987&t=pageview&_s=1&dl=https%3A%2F%2Fizhevsk.domru.ru%2Fbundles&dr=https%3A%2F%2Fwww.google.com%2F&ul=ru-ru&de=UTF-8&dt=%D0%A2%D0%B0%D1%80%D0%B8%D1%84%D1%8B%20%D0%B4%D0%BE%D0%BC%D0%B0%D1%88%D0%BD%D0%B5%D0%B3%D0%BE%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%D0%B0%20%D0%B8%20%D0%A2%D0%92%20%D0%BE%D1%82%20%D0%94%D0%BE%D0%BC.ru%20%D0%B2%20%D0%98%D0%B6%D0%B5%D0%B2%D1%81%D0%BA%D0%B5%20%7C%20%D0%92%D1%8B%D0%B3%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%20%D0%BF%D0%B0%D0%BA%D0%B5%D1%82%D1%8B%20%D0%BD%D0%B0%20%D0%BF%D0%BE%D0%B4%D0%BA%D0%BB%D1%8E%D1%87%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%D0%B0%2C%20%D1%82%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D1%8F&sd=24-bit&sr=1920x1080&vp=1885x952&je=0&_u=yCEAiEABBAAAi~&jid=&gjid=&cid=826693589.1574067108&tid=UA-42895529-1&_gid=1495280721.1574067108&gtm=2wgav3M43KXLC&cd5=https%3A%2F%2Fwww.google.com%2F&cd7=826693589.1574067108&z=1582172344	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36	uid=		p_uid=589CBABC3B5BD25D024E3DAD02F2091B	http_ref=https://izhevsk.domru.ru/bundles	izhevsk"""
    val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
    assert(p.nonEmpty)
  }

  test("test 20191129 failed package on v05 - Too big Int in pagedownloadtime") {
    val deserializer = new PixelDomRuPagetrackingDeserializer(null)
    val testValue = """2019-11-18T17:41:27+00:00	5.18.249.115	/stat.gif?v=1&_v=j79&a=1742095975&t=adtiming&_s=2&dl=https%3A%2F%2Finterzet.domru.ru%2Fbundles_land_new%3Ftext%3Dvar_2_search_adaptive%26utm_source%3Dgoogle%26utm_medium%3Dcpc%26utm_campaign%3Db2c_spb_brand_search%26utm_content%3D__5gqd5-gq6y0_-cid727756443-gid37919080557-ad304873470365-crtkwd-36412186638_%26utm_term%3D%25D0%25B4%25D0%25BE%25D0%25BC%25D1%2580%25D1%2583%26gclid%3DEAIaIQobChMIrLuM_OzS5QIVVKmaCh1Yjwj6EAAYASAAEgIgAPD_BwE&dr=https%3A%2F%2Fwww.google.ru%2F&ul=ru-ru&de=UTF-8&dt=%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%20%D0%B8%20%D1%82%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%BE%D1%82%20%D0%94%D0%BE%D0%BC.ru%20%D0%B2%20%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3%D0%B5&sd=32-bit&sr=320x568&vp=320x460&je=0&plt=9315&pdt=1574061551174&dns=9&rrt=0&srt=355&tcp=115&dit=303&clt=395&_gst=2963&_gbt=3704&_cst=2491&_cbt=3252&_u=6CHAiEQABAAAg~&jid=&gjid=&cid=2142519972.1572949364&tid=UA-42895529-1&_gid=591310789.1574098990&gtm=2wgan1M43KXLC&z=2018681379	Mozilla/5.0 (iPhone; CPU iPhone OS 10_2 like Mac OS X) AppleWebKit/602.3.12 (KHTML, like Gecko) Version/10.0 Mobile/14C92 Safari/602.1	uid=		p_uid=589CBABC104DC15D024E3DAD023EEF6A	http_ref=https://interzet.domru.ru/bundles_land_new?text=var_2_search_adaptive&utm_source=google&utm_medium=cpc&utm_campaign=b2c_spb_brand_search&utm_content=__5gqd5-gq6y0_-cid727756443-gid37919080557-ad304873470365-crtkwd-36412186638_&utm_term=%D0%B4%D0%BE%D0%BC%D1%80%D1%83&gclid=EAIaIQobChMIrLuM_OzS5QIVVKmaCh1Yjwj6EAAYASAAEgIgAPD_BwE	"""
    val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
    assert(p.isEmpty)
  }

  /* keep this test the last one, this makes possible to turn off try-catch block and get exception on exact error line */
  test("test EmptyTsv") {
    val deserializer = new PixelDomRuPagetrackingDeserializer(null)
    val testValue = ""
    val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
    assert(p.isEmpty)
  }

  test("feature: BIGD-873: # in url") {
    val deserializer = new PixelDomRuPagetrackingDeserializer(null)
    val testValue = """2020-08-30T08:24:33+00:00	213.87.154.50	/stat.gif?v=1&_v=j85&a=1976917606&t=pageview&_s=1&dl=https%3A%2F%2Fmsk.domru.ru%2Fvideocontrol%23utm_source%3DYandex_Dzen%26utm_medium%3Dpackage%26utm_campaign%3DOLV_Dzen_June-Aug_2020%26utm_content%3DDzen_video_surveillance%26utm_term%3D__e0pyq-wvd5q_&dr=https%3A%2F%2Fzen.yandex.ru%2Fmedia%2Fid%2F5d5683e9ae56cc00ac522f1f%2Fchto-nujno-znat-pro-domashnee-videonabliudenie-chtoby-ne-boiatsia-ego-podkliuchit-5ef4dca76855c736708599c9&ul=ru-ru&de=UTF-8&dt=%D0%A1%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B0%20%D0%B2%D0%B8%D0%B4%D0%B5%D0%BE%D0%BD%D0%B0%D0%B1%D0%BB%D1%8E%D0%B4%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%B4%D0%BB%D1%8F%20%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B8%D1%80%D1%8B%20%D1%87%D0%B5%D1%80%D0%B5%D0%B7%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%20%7C%20%D0%A3%D1%81%D1%82%D0%B0%D0%BD%D0%BE%D0%B2%D0%BA%D0%B0%20%D0%BA%D0%B0%D0%BC%D0%B5%D1%80%D1%8B%20%D0%B2%D0%B8%D0%B4%D0%B5%D0%BE%D0%BD%D0%B0%D0%B1%D0%BB%D1%8E%D0%B4%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%B2%20%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B5&sd=24-bit&sr=1366x768&vp=1349x625&je=0&_u=6GHAiEABBAAAAC~&jid=1181860521&gjid=1118823600&cid=408451406.1598775871&tid=UA-42895529-1&_gid=1589799509.1598775871&gtm=2wg8j2M43KXLC&cd5=https%3A%2F%2Fzen.yandex.ru%2Fmedia%2Fid%2F5d5683e9ae56cc00ac522f1f%2Fchto-nujno-znat-pro-domashnee-videonabliudenie-chtoby-ne-boiatsia-ego-podkliuchit-5ef4dca76855c736708599c9&cd7=408451406.1598775871&z=981579671&ploc=https%3A%2F%2Fmsk.domru.ru%2Fvideocontrol%23main%3Futm_source%3DYandex_Dzen%26utm_medium%3Dpackage%26utm_campaign%3DOLV_Dzen_June-Aug_2020%26utm_content%3DDzen_video_surveillance%26utm_term%3D__e0pyq-wvd5q_	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36	uid=	p_uid=589CBABC91A6345E441313A7028F9D31	http_ref=https://msk.domru.ru/videocontrol	msk"""
    val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))

    assert(p.nonEmpty)
    assert("https://msk.domru.ru/videocontrol#main?utm_source=Yandex_Dzen&utm_medium=package&utm_campaign=OLV_Dzen_June-Aug_2020&utm_content=Dzen_video_surveillance&utm_term=__e0pyq-wvd5q_" == p.get.url)
  }

  test("feature: BIGD-884: add b2b cookie") {
    val deserializer = new PixelDomRuPagetrackingDeserializer(null)
    val testValue = """2020-09-09T13:27:07+00:00	188.65.245.40	/statb.gif?v=1&_v=j85&a=883007655&t=pageview&_s=1&dl=https%3A%2F%2Flkb2b.domru.ru%2Fpay%2Fresult%3Fpartner_id%3D1100925%26order_number%3D556Z7QNDDGGDPB%26payment_id%3D113498006%26city_id%3D556%26payment_type%3D2%26orderId%3Da12193e9-8ef2-7b27-8700-3a710014a5d4%26lang%3Dru&dr=https%3A%2F%2Facs3.sbrf.ru&ul=ru&de=UTF-8&dt=%D0%A0%D0%B5%D0%B7%D1%83%D0%BB%D1%8C%D1%82%D0%B0%D1%82%20%D0%BE%D0%BF%D0%BB%D0%B0%D1%82%D1%8B%20%7C%20%D0%9B%D0%B8%D1%87%D0%BD%D1%8B%D0%B9%20%D0%BA%D0%B0%D0%B1%D0%B8%D0%BD%D0%B5%D1%82%20B2B%20%D0%94%D0%BE%D0%BC.ru&sd=32-bit&sr=414x736&vp=414x622&je=0&_u=QCCAgAAB~&jid=1689633114&gjid=385078724&cid=408440991.1599657857&uid=interzet100780030147498&tid=UA-42532108-1&_gid=857775723.1599657857&gtm=2wg8q1MVZV2&cd1=interzet100780030147498&cd2=408440991.1599657857&cd3=spb&z=1657604626	Mozilla/5.0 (iPhone; CPU iPhone OS 13_6_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Mobile/15E148 Safari/604.1	uid=		p_uid=589CBABC8511AB5D024E3DAD0217D99B	http_ref=https://lkb2b.domru.ru/pay/result?partner_id=1100925&order_number=556Z7QNDDGGDPB&payment_id=113498006&city_id=556&payment_type=2&orderId=a12193e9-8ef2-7b27-8700-3a710014a5d4&lang=ru	spb	interzet100780030147498"""
    val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))

    assert(p.nonEmpty)
    assert(Some("spb") == p.get.citydomain)
    assert(Some("100780030147498") == p.get.client_id_b2b)
  }

}

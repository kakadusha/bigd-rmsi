package ru.ertelecom.kafka.extract.web_domru_sources.serde

import java.nio.charset.Charset
import org.scalatest.FunSuite

import java.net.URL

class PixelDomRuPagetrackingDeserializerTest extends FunSuite {

    test("scala map") {
        val testMap = Map("utm_source" -> "test", "utm_medium" -> "organic")
        assert("test" == testMap.get("utm_source").get)
    }

    test("utm: ref") {
        val ref = Some("https://www.google.com/")
        var utm:Option[Map[String, String]] = ref match {
            case Some(q) if (!q.isEmpty) => Some(Map("utm_source" -> new URL(q).getHost(), "utm_medium" -> "organic"))
            case _ => Option.empty
        }

        assert("www.google.com" == utm.get("utm_source"))
    }

    test("utm: ref empty") {
        val ref = Some("")
        var utm:Option[Map[String, String]] = ref match {
            case Some(q) if (!q.isEmpty) => Some(Map("utm_source" -> new URL(q).getHost(), "utm_medium" -> "organic"))
            case _ => Option.empty
        }

        assert(utm.isEmpty)
    }

    test("utm: url") {
        val deserializer = new WebDomruSourceDeserializer(null)

        val url = Some("https://nsk.domru.ru/bundles_land_new?utm_source=google&utm_medium=cpc&utm_campaign=b2c_smart_display_all&utm_content=cid9849255748-gid105734971408-ad443883308575-crt_&utm_term=&gclid=EAIaIQobChMIxvTws_S66gIVQUXCCh1WtQ2wEAEYASAAEgKANfD_BwE")
        var utm:Map[String, String] = url match {
                case Some(v) if (!v.isEmpty && v.contains("utm_source") ) => deserializer.queryToMap(v)
                case _ => Map.empty[String, String]
        }

        assert("google" == utm.get("utm_source").get)
    }

    test("utm: url empty") {
        val deserializer = new WebDomruSourceDeserializer(null)

        val url = Some("")
        var utm:Map[String, String] = url match {
                case Some(v) if (!v.isEmpty && v.contains("utm_source") ) => deserializer.queryToMap(v)
                case _ => Map.empty[String, String]
        }

        assert(utm.isEmpty)
    }

    test("utm: url not empty") {
        val deserializer = new WebDomruSourceDeserializer(null)

        val url = Some("https://izhevsk.domru.ru/bundles")
        var utm:Map[String, String] = url match {
                case Some(v) if (!v.isEmpty && v.contains("utm_source") ) => deserializer.queryToMap(v)
                case _ => Map.empty[String, String]
        }

        assert(utm.isEmpty)
    }

    test("utm: url + ref") {
        val deserializer = new WebDomruSourceDeserializer(null)

        val url = Some("https://izhevsk.domru.ru/bundles")
        val ref = Some("https://www.google.com/")
        var utm:Option[Map[String, String]] = ref match {
            case Some(q) if (!q.isEmpty) => Some(Map("utm_source" -> new URL(q).getHost(), "utm_medium" -> "organic"))
            case _ => Option.empty
        }
        if (utm.isEmpty)
            utm = ref match {
                case Some(q) if !q.isEmpty => Some(Map("utm_source" -> new URL(q).getHost(), "utm_medium" -> "organic"))
                case _ => Option.empty
            }

        assert("www.google.com" == utm.get("utm_source"))
    }

    test("test utm - ref") {
        val deserializer = new WebDomruSourceDeserializer(null)
        val testValue = """2019-11-18T08:54:29+00:00	188.233.239.208	/stat.gif?v=1&_v=j79&a=1063434987&t=pageview&_s=1&dl=https%3A%2F%2Fizhevsk.domru.ru%2Fbundles&dr=https%3A%2F%2Fwww.google.com%2F&ul=ru-ru&de=UTF-8&dt=%D0%A2%D0%B0%D1%80%D0%B8%D1%84%D1%8B%20%D0%B4%D0%BE%D0%BC%D0%B0%D1%88%D0%BD%D0%B5%D0%B3%D0%BE%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%D0%B0%20%D0%B8%20%D0%A2%D0%92%20%D0%BE%D1%82%20%D0%94%D0%BE%D0%BC.ru%20%D0%B2%20%D0%98%D0%B6%D0%B5%D0%B2%D1%81%D0%BA%D0%B5%20%7C%20%D0%92%D1%8B%D0%B3%D0%BE%D0%B4%D0%BD%D1%8B%D0%B5%20%D0%BF%D0%B0%D0%BA%D0%B5%D1%82%D1%8B%20%D0%BD%D0%B0%20%D0%BF%D0%BE%D0%B4%D0%BA%D0%BB%D1%8E%D1%87%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%D0%B0%2C%20%D1%82%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D1%8F&sd=24-bit&sr=1920x1080&vp=1885x952&je=0&_u=yCEAiEABBAAAi~&jid=&gjid=&cid=826693589.1574067108&tid=UA-42895529-1&_gid=1495280721.1574067108&gtm=2wgav3M43KXLC&cd5=https%3A%2F%2Fwww.google.com%2F&cd7=826693589.1574067108&z=1582172344	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36	uid=		p_uid=589CBABC3B5BD25D024E3DAD02F2091B	http_ref=https://izhevsk.domru.ru/bundles	izhevsk"""
        val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
        assert(p.nonEmpty)
    }

    test("test utm - url") {
        val deserializer = new WebDomruSourceDeserializer(null)
        val testValue = """2019-11-18T17:41:27+00:00	5.18.249.115	/stat.gif?v=1&_v=j79&a=1742095975&t=adtiming&_s=2&dl=https%3A%2F%2Finterzet.domru.ru%2Fbundles_land_new%3Ftext%3Dvar_2_search_adaptive%26utm_source%3Dgoogle%26utm_medium%3Dcpc%26utm_campaign%3Db2c_spb_brand_search%26utm_content%3D__5gqd5-gq6y0_-cid727756443-gid37919080557-ad304873470365-crtkwd-36412186638_%26utm_term%3D%25D0%25B4%25D0%25BE%25D0%25BC%25D1%2580%25D1%2583%26gclid%3DEAIaIQobChMIrLuM_OzS5QIVVKmaCh1Yjwj6EAAYASAAEgIgAPD_BwE&dr=https%3A%2F%2Fwww.google.ru%2F&ul=ru-ru&de=UTF-8&dt=%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%20%D0%B8%20%D1%82%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%BE%D1%82%20%D0%94%D0%BE%D0%BC.ru%20%D0%B2%20%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3%D0%B5&sd=32-bit&sr=320x568&vp=320x460&je=0&plt=9315&pdt=1574061551174&dns=9&rrt=0&srt=355&tcp=115&dit=303&clt=395&_gst=2963&_gbt=3704&_cst=2491&_cbt=3252&_u=6CHAiEQABAAAg~&jid=&gjid=&cid=2142519972.1572949364&tid=UA-42895529-1&_gid=591310789.1574098990&gtm=2wgan1M43KXLC&z=2018681379	Mozilla/5.0 (iPhone; CPU iPhone OS 10_2 like Mac OS X) AppleWebKit/602.3.12 (KHTML, like Gecko) Version/10.0 Mobile/14C92 Safari/602.1	uid=		p_uid=589CBABC104DC15D024E3DAD023EEF6A	http_ref=https://interzet.domru.ru/bundles_land_new?text=var_2_search_adaptive&utm_source=google&utm_medium=cpc&utm_campaign=b2c_spb_brand_search&utm_content=__5gqd5-gq6y0_-cid727756443-gid37919080557-ad304873470365-crtkwd-36412186638_&utm_term=%D0%B4%D0%BE%D0%BC%D1%80%D1%83&gclid=EAIaIQobChMIrLuM_OzS5QIVVKmaCh1Yjwj6EAAYASAAEgIgAPD_BwE	"""
        val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
        assert(p.nonEmpty)

        assert("google" == p.get.utm_source)
        assert("cpc" == p.get.utm_medium)
        assert(Some("b2c_spb_brand_search") == p.get.utm_campaign)
        assert(Some("__5gqd5-gq6y0_-cid727756443-gid37919080557-ad304873470365-crtkwd-36412186638_") == p.get.utm_content)
        assert(Some("EAIaIQobChMIrLuM_OzS5QIVVKmaCh1Yjwj6EAAYASAAEgIgAPD_BwE") == p.get.gclid)
    }

    test("test utm - emprty") {
        val deserializer = new WebDomruSourceDeserializer(null)
        val testValue = """2019-11-18T17:41:27+00:00	5.18.249.115	/stat.gif?v=1&_v=j79&a=1742095975&t=adtiming&_s=2&dl=https%3A%2F%2Fizhevsk.domru.ru%2Fbundles=&dr=https%3A%2F%2Fizhevsk.domru.ru%2Fbundles=&ul=ru-ru&de=UTF-8&dt=%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%20%D0%B8%20%D1%82%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%BE%D1%82%20%D0%94%D0%BE%D0%BC.ru%20%D0%B2%20%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3%D0%B5&sd=32-bit&sr=320x568&vp=320x460&je=0&plt=9315&pdt=1574061551174&dns=9&rrt=0&srt=355&tcp=115&dit=303&clt=395&_gst=2963&_gbt=3704&_cst=2491&_cbt=3252&_u=6CHAiEQABAAAg~&jid=&gjid=&cid=2142519972.1572949364&tid=UA-42895529-1&_gid=591310789.1574098990&gtm=2wgan1M43KXLC&z=2018681379	Mozilla/5.0 (iPhone; CPU iPhone OS 10_2 like Mac OS X) AppleWebKit/602.3.12 (KHTML, like Gecko) Version/10.0 Mobile/14C92 Safari/602.1	uid=		p_uid=589CBABC104DC15D024E3DAD023EEF6A	http_ref=https://interzet.domru.ru/bundles_land_new?text=var_2_search_adaptive&utm_source=google&utm_medium=cpc&utm_campaign=b2c_spb_brand_search&utm_content=__5gqd5-gq6y0_-cid727756443-gid37919080557-ad304873470365-crtkwd-36412186638_&utm_term=%D0%B4%D0%BE%D0%BC%D1%80%D1%83&gclid=EAIaIQobChMIrLuM_OzS5QIVVKmaCh1Yjwj6EAAYASAAEgIgAPD_BwE	"""
        val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
        assert(p.isEmpty)
    }

    /* keep this test the last one, this makes possible to turn off try-catch block and get exception on exact error line */
    test("test EmptyTsv") {
        val deserializer = new WebDomruSourceDeserializer(null)
        val testValue = ""
        val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
        assert(p.isEmpty)
    }

    test("bug: BIGD-867: url 0") {
        val deserializer = new WebDomruSourceDeserializer(null)

        val url = Some("https://nn.domru.ru/internet?hide&dop=link&?utm_source=google&utm_medium=cpc&utm_campaign=b2c_nn_brand_search&utm_content=__r93vn-wpgxq_-cid277755366-gid17706210966-ad450626870796-crtkwd-16555238409_&utm_term=%D0%B4%D0%BE%D0%BC%20%D1%80%D1%83&gclid=EAIaIQobChMI64admKiv6wIVh_uyCh2kBgu3EAAYASACEgISpfD_BwE&ul=ru&de=UTF-8")
        var utm:Map[String, String] = url match {
                case Some(v) if (!v.isEmpty && v.contains("utm_source") ) => deserializer.queryToMap(v)
                case _ => Map.empty[String, String]
        }

        assert("google" == utm.get("utm_source").get)
    }

    test("bug: BIGD-867: url 1") {
        val deserializer = new WebDomruSourceDeserializer(null)

        val url = Some("https://irkutsk.domru.ru/bundles_land_new?text=var_3_search_adaptive&&utm_source=google&gclid=EAIaIQobChMIwJi2oL-x6wIVxtmyCh2dBw9rEAAYASAAEgIqRPD_BwE")
        var utm:Map[String, String] = url match {
                case Some(v) if (!v.isEmpty && v.contains("utm_source") ) => deserializer.queryToMap(v)
                case _ => Map.empty[String, String]
        }

        assert("google" == utm.get("utm_source").get)
    }

    test("bug: BIGD-867: url 2") {
        val deserializer = new WebDomruSourceDeserializer(null)

        val url = Some("https://cheb.domru.ru/digitaltv?&utm_source=google&gclid=EAIaIQobChMIwI_ds6u26wIVh9OyCh0pHgFeEAAYASAAEgIVNvD_BwE")
        var utm:Map[String, String] = url match {
                case Some(v) if (!v.isEmpty && v.contains("utm_source") ) => deserializer.queryToMap(v)
                case _ => Map.empty[String, String]
        }

        assert("google" == utm.get("utm_source").get)
    }

    test("bug: BIGD-867: all") {
        val deserializer = new WebDomruSourceDeserializer(null)
        
        val testValue = """2020-08-24T18:57:39+00:00	85.140.2.229	/stat.gif?v=1&_v=j83&a=954424715&t=pageview&_s=1&dl=https%3A%2F%2Fnn.domru.ru%2Finternet%3Fhide%26dop%3Dlink%26%3Futm_source%3Dgoogle%26utm_medium%3Dcpc%26utm_campaign%3Db2c_nn_brand_search%26utm_content%3D__r93vn-wpgxq_-cid277755366-gid17706210966-ad450626870796-crtkwd-16555238409_%26utm_term%3D%25D0%25B4%25D0%25BE%25D0%25BC%2520%25D1%2580%25D1%2583%26gclid%3DEAIaIQobChMI64admKiv6wIVh_uyCh2kBgu3EAAYASACEgISpfD_BwE&ul=ru&de=UTF-8&dt=%D0%A2%D0%B0%D1%80%D0%B8%D1%84%D1%8B%20%D0%B4%D0%BE%D0%BC%D0%B0%D1%88%D0%BD%D0%B5%D0%B3%D0%BE%20%D0%B1%D0%B5%D0%B7%D0%BB%D0%B8%D0%BC%D0%B8%D1%82%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%D0%B0%20%D0%BE%D1%82%20%D0%94%D0%BE%D0%BC.ru%20%D0%B2%20%D0%9D%D0%B8%D0%B6%D0%BD%D0%B5%D0%BC%20%D0%9D%D0%BE%D0%B2%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D0%B5%20%7C%20%D0%9F%D0%BE%D0%B4%D0%BA%D0%BB%D1%8E%D1%87%D0%B8%D1%82%D1%8C%20%D0%BF%D0%B0%D0%BA%D0%B5%D1%82%D1%8B%20%D1%83%D1%81%D0%BB%D1%83%D0%B3%20%D0%BF%D0%BE%20%D0%B2%D1%8B%D0%B3%D0%BE%D0%B4%D0%BD%D0%BE%D0%B9%20%D1%81%D1%82%D0%BE%D0%B8%D0%BC%D0%BE%D1%81%D1%82%D0%B8&sd=32-bit&sr=414x736&vp=414x622&je=0&_u=6HHAiEABBAAAgC~&jid=802831133&gjid=257844907&cid=1229405150.1598295459&tid=UA-42895529-1&_gid=1157172890.1598295459&gtm=2wg8c0M43KXLC&cd5=&cd7=1229405150.1598295459&z=1363227785	Mozilla/5.0 (iPhone; CPU iPhone OS 13_5_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1	uid=	p_uid=589CBABCF852415F46323D120292C748	http_ref=https://nn.domru.ru/internet?hide&dop=link&?utm_source=google&utm_medium=cpc&utm_campaign=b2c_nn_brand_search&utm_content=__r93vn-wpgxq_-cid277755366-gid17706210966-ad450626870796-crtkwd-16555238409_&utm_term=%D0%B4%D0%BE%D0%BC%20%D1%80%D1%83&gclid=EAIaIQobChMI64admKiv6wIVh_uyCh2kBgu3EAAYASACEgISpfD_BwE	nn"""
        val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))

        assert(p.nonEmpty)
    }

    test("feature: BIGD-873: all") {
        val deserializer = new WebDomruSourceDeserializer(null)
        
        val testValue = """2020-09-01T08:53:59+00:00	92.255.193.226	/stat.gif?v=1&_v=j85&a=1226819067&t=pageview&_s=1&dl=https%3A%2F%2Fvolgograd.domru.ru%2Fbundles_land_new%3Ftext%3Dvar_2_search_adaptive%26%3Futm_source%3Dgoogle%26utm_medium%3Dcpc%26utm_campaign%3Db2c_volgograd_brand_search%26utm_content%3D__r93vn-wpgxq_-cid279208326-gid17779951926-ad450626871513-crtkwd-42095587130_%26utm_term%3D%252Bdom%2520%252Bru%26gclid%3DCjwKCAjw4rf6BRAvEiwAn2Q76h80sw7AV_5PdZ1KZPoN8V10kuD5i9YW73i0DfDnOfOsrqE00Pi3dBoCR3gQAvD_BwE&ul=ru&de=UTF-8&dt=%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%20%D0%B8%20%D1%82%D0%B5%D0%BB%D0%B5%D0%B2%D0%B8%D0%B4%D0%B5%D0%BD%D0%B8%D0%B5%20%D0%BE%D1%82%20%D0%94%D0%BE%D0%BC.ru%20%D0%B2%20%D0%92%D0%BE%D0%BB%D0%B3%D0%BE%D0%B3%D1%80%D0%B0%D0%B4%D0%B5&sd=24-bit&sr=424x753&vp=424x673&je=0&_u=yCEAiEABBAAAgC~&jid=1891602484&gjid=863084595&cid=367711634.1567415724&tid=UA-42895529-1&_gid=368019579.1598949934&gtm=2wg8j2M43KXLC&cd5=&cd7=367711634.1567415724&z=2138151010&ploc=https%3A%2F%2Fvolgograd.domru.ru%2Fbundles_land_new%3Ftext%3Dvar_2_search_adaptive%26%3Futm_source%3Dgoogle%26utm_medium%3Dcpc%26utm_campaign%3Db2c_volgograd_brand_search%26utm_content%3D__r93vn-wpgxq_-cid279208326-gid17779951926-ad450626871513-crtkwd-42095587130_%26utm_term%3D%252Bdom%2520%252Bru%26gclid%3DCjwKCAjw4rf6BRAvEiwAn2Q76h80sw7AV_5PdZ1KZPoN8V10kuD5i9YW73i0DfDnOfOsrqE00Pi3dBoCR3gQAvD_BwE	Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36	uid=	p_uid=589CBABC871A1F5D7079AB0B021F3903	http_ref=https://kazan.domru.ru/domru-tv/packages/amedia-premium	kazan"""
        val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))

        assert(p.nonEmpty)
    }

    test("feature: BIGD-873: # in url") {
        val deserializer = new WebDomruSourceDeserializer(null)
        
        val testValue = """2020-08-30T08:24:33+00:00	213.87.154.50	/stat.gif?v=1&_v=j85&a=1976917606&t=pageview&_s=1&dl=https%3A%2F%2Fmsk.domru.ru%2Fvideocontrol%23utm_source%3DYandex_Dzen%26utm_medium%3Dpackage%26utm_campaign%3DOLV_Dzen_June-Aug_2020%26utm_content%3DDzen_video_surveillance%26utm_term%3D__e0pyq-wvd5q_&dr=https%3A%2F%2Fzen.yandex.ru%2Fmedia%2Fid%2F5d5683e9ae56cc00ac522f1f%2Fchto-nujno-znat-pro-domashnee-videonabliudenie-chtoby-ne-boiatsia-ego-podkliuchit-5ef4dca76855c736708599c9&ul=ru-ru&de=UTF-8&dt=%D0%A1%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B0%20%D0%B2%D0%B8%D0%B4%D0%B5%D0%BE%D0%BD%D0%B0%D0%B1%D0%BB%D1%8E%D0%B4%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%B4%D0%BB%D1%8F%20%D0%BA%D0%B2%D0%B0%D1%80%D1%82%D0%B8%D1%80%D1%8B%20%D1%87%D0%B5%D1%80%D0%B5%D0%B7%20%D0%B8%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82%20%7C%20%D0%A3%D1%81%D1%82%D0%B0%D0%BD%D0%BE%D0%B2%D0%BA%D0%B0%20%D0%BA%D0%B0%D0%BC%D0%B5%D1%80%D1%8B%20%D0%B2%D0%B8%D0%B4%D0%B5%D0%BE%D0%BD%D0%B0%D0%B1%D0%BB%D1%8E%D0%B4%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%B2%20%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B5&sd=24-bit&sr=1366x768&vp=1349x625&je=0&_u=6GHAiEABBAAAAC~&jid=1181860521&gjid=1118823600&cid=408451406.1598775871&tid=UA-42895529-1&_gid=1589799509.1598775871&gtm=2wg8j2M43KXLC&cd5=https%3A%2F%2Fzen.yandex.ru%2Fmedia%2Fid%2F5d5683e9ae56cc00ac522f1f%2Fchto-nujno-znat-pro-domashnee-videonabliudenie-chtoby-ne-boiatsia-ego-podkliuchit-5ef4dca76855c736708599c9&cd7=408451406.1598775871&z=981579671&ploc=https%3A%2F%2Fmsk.domru.ru%2Fvideocontrol%23main%3Futm_source%3DYandex_Dzen%26utm_medium%3Dpackage%26utm_campaign%3DOLV_Dzen_June-Aug_2020%26utm_content%3DDzen_video_surveillance%26utm_term%3D__e0pyq-wvd5q_	Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36	uid=	p_uid=589CBABC91A6345E441313A7028F9D31	http_ref=https://msk.domru.ru/videocontrol	msk"""
        val p = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))

        assert(p.nonEmpty)
        assert(Some("https://msk.domru.ru/videocontrol#main?utm_source=Yandex_Dzen&utm_medium=package&utm_campaign=OLV_Dzen_June-Aug_2020&utm_content=Dzen_video_surveillance&utm_term=__e0pyq-wvd5q_") == p.get.url)
    }
}

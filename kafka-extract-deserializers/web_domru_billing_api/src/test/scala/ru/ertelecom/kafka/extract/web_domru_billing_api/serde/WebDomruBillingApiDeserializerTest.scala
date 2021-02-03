package ru.ertelecom.kafka.extract.web_domru_billing_api.serde

import java.nio.charset.StandardCharsets
import org.scalatest.FunSuite

import ru.ertelecom.kafka.extract.web_domru_billing_api.domain.WebDomruBillingApi

class WebDomruBillingApiDeserializerTest extends FunSuite {

    test("WebDomruBillingApi test") {

        val payload = """   timestamp="30/Jun/2020:14:07:56 +0300" remote_addr="188.186.153.95" x_forwarded_for="2a02:2698:2806:4b57:8526:57dd:838c:1267" x_real_ip="-" http_status="200" user_agent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36 OPR/68.0.3618.173" x_request_id="XvsdCwpj9jAAAEpAdNQAAAAp" request_time="0.444" response_time="0.444" upstream_addr="10.99.246.48:8002" apistatus="1" platform="lk" apiname="rbs_binding_list" api_errorcode="-" agreementnumber="160008477006" args="access_token=4pqt9dj9ase9qsa8w36wn9h8d0n2wv&params=rbs_binding_list" pkg="web_cabinet.get_info" city="kazan" oauthclient="WEB_CABINET_LONG" system="elk"
        """.stripMargin.getBytes

        val deserializer = new WebDomruBillingApiDeserializer(null)        
        val webDomruBillingApi: Option[WebDomruBillingApi] = deserializer.parseCsv(payload)

        assert(webDomruBillingApi.isDefined)

        assert("2a02:2698:2806:4b57:8526:57dd:838c:1267" == webDomruBillingApi.get.remote_addr)
        assert(0.444 == webDomruBillingApi.get.request_time)
        assert(Some("access_token=4pqt9dj9ase9qsa8w36wn9h8d0n2wv&params=rbs_binding_list") == webDomruBillingApi.get.args)
    }

    test("WebDomruBillingApi ipv6 test") {

        val payload = """timestamp="30/Jun/2020:16:35:33 +0500" remote_addr="91.144.185.129" x_forwarded_for="2a00:1fa2:20e:3f89:9587:5693:1a63:d41e" x_real_ip="2a00:1fa2:20e:3f89:9587:5693:1a63:d41e" http_status="200" user_agent="okhttp/3.12.0" x_request_id="XvsjhApj6y8AAF97FUEAAAAU" request_time="0.661" response_time="0.661" upstream_addr="10.99.235.47:8002" apistatus="1" platform="my_domru" apiname="check_can_service_req_create" api_errorcode="-" agreementnumber="660024176741" args="access_token=3vn3wbngh9s9ir24jzgzjkd20pfdg3&params=check_can_service_req_create" pkg="web_cabinet.get_info" city="ekat" oauthclient="DOMRU_AGENT" system="elk"
        """.stripMargin.getBytes

        val deserializer = new WebDomruBillingApiDeserializer(null)        
        val webDomruBillingApi: Option[WebDomruBillingApi] = deserializer.parseCsv(payload)

        assert(webDomruBillingApi.isDefined)

        assert("2a00:1fa2:20e:3f89:9587:5693:1a63:d41e" == webDomruBillingApi.get.remote_addr)
    }
}

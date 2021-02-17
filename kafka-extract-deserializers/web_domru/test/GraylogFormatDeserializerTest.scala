package ru.ertelecom.kafka.extract.web_domru.serde

import java.nio.charset.StandardCharsets
import org.scalatest.FunSuite

class GraylogFormatDeserializerTest extends FunSuite {
  test("Graylog Format Deserializer Test") {
    val payload = ' { "timestamp": "2020-05-21T23:03:36+05:00", "remote_addr": "85.113.63.236", "body_bytes_sent": 23388, "request_time": 0.000, "response_status": 200, "request": "GET /themes/default/css/style.css?v=160 HTTP/1.0", "request_method": "GET", "host": "samara.wifi.domru.ru","upstream_cache_status": "HIT","upstream_addr": "","http_x_forwarded_for": "10.12.21.170","http_referrer": "https://samara.wifi.domru.ru/index.php?request_uri=http://connect.rom.miui.com/generate_204","upstream_response_time": "","upstream_header_time": "","upstream_connect_time": "","request_id": "44b6b4b2b18e6e544d95eedb454f60c9","http_user_agent": "Mozilla/5.0 (Linux; Android 10; Mi 9T Build/QKQ1.190825.002; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/81.0.4044.138 Mobile Safari/537.36","x-project": "","lb-fqdn": "balancer3.site3.ertelecom.ru","container_id": "","citydomain": "","userId": "","uid_set": "","uid_got": "vLqcWF6X359+S0AgKv/8Ag==","request_body": "" }'.stripMargin.getBytes
    val deserializer = new GraylogFormatDeserializer(null)
    val graylogFormatDes = deserializer.parseJson(payload)

    assert(deserializer.parseJson(payload).isDefined)
  }
}

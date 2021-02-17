package ru.ertelecom.kafka.extract.tv_device.serde

import java.nio.charset.Charset

import org.scalatest.FunSuite

class TvDeviceDeserializerTest extends FunSuite{
  test("testSuccess") {
    val testSample = "<190>Nov 18 14:15:23 discovery2 balancer: @timestamp=2019-11-18T14:15:23+04:00\tserver.name=discovery2\t"+
      "server.point=samara\tserver.type=balancer\tserver.number_connection=116448769\tserver.connection_requests=18\trequest.time=18/Nov/2019:14:15:23 +0400\t" +
      "request.sxema=http\trequest.metod=GET\trequest.host=discovery-h9k-21.ertelecom.ru\t" +
      "request.block_location=collection\trequest.block_location_name=vod.asset\trequest.location=/collection/vo\t" +
      "request.real_url=/collection/vod.asset/query/dimension/er_lcn/in/18/dimension/type/eq/channel/dimension/is_epg_available/eq/1\t" +
      "request.internal_url=/collection/vod.asset/query/dimension/er_lcn/in/18/dimension/type/eq/chann\t" +
      "request.body=\trequest.body_byte_in=\trequest.ip=5.165.116.29\trequest.port=32596\trequest.user=\t" +
      "request.referer=\trequest.agent=Opera/9.80 (Linux mips; Opera TV Store/6025; ce-html/1.0)\trequest.forward=\t" +
      "request.protocol=HTTP/1.1\trequest.x_app_version=\trequest.completion=OK\trespons\tresponse.body_byte_sent=5614\t" +
      "response.time=0.000\tresponse.upstream_addrs=\tresponse.upstream_connect_time=0\tresponse.upstream_header_time=0 " +
      "response.upstream_response_time=0\tresponse.upstream_conne\tresponse.upstream_header_time_string=[] " +
      "response.upstream_response_time_string=[]\tresponse.upstream_status_code_string=[]\tresponse.cache_status=1\t" +
      "response.cache_group=platform_and_group\tresponse.upstream_x-res\tresponse.upstream_tag_error=\tresponse.upstream_tag_error_code=\t" +
      "response.lua_response=0\tplatform.id=4\tplatform.extid=stb\tsubscriber.is_guest=0\tsubscriber.id=871157\t" +
      "subscriber.extid=lipetsk:480006304328\ts\tsubscriber.token_is_expires=0\tsubscriber.operator_id=2\tsubscriber.operator_extid=er\t" +
      "subscriber.groups_title=er:domain:lipetsk\tsubscriber.groups_id=35016\tdevice.id=871128\tdevice.extid=nuid:300"
    val deserializer = new TvDeviceDeserializer(null)
    val tvDeviceOpt = deserializer.parse(testSample.getBytes(Charset.forName("UTF-8")))
    assert(tvDeviceOpt.isDefined)
    val tvDevice = tvDeviceOpt.get
    assert(1574072123L == tvDevice.ts)
    assert("discovery2" == tvDevice.server_name)
    assert("samara" == tvDevice.server_point)
    assert("balancer" == tvDevice.server_type)
    assert(116448769L == tvDevice.server_num_conn)
    assert(18 == tvDevice.server_conn_req.get)
    assert(1574072123L == tvDevice.req_time.get)
    assert("http" == tvDevice.req_schema.get)
    assert("GET" == tvDevice.req_method.get)
    assert("discovery-h9k-21.ertelecom.ru" == tvDevice.req_host.get)
    assert("collection" == tvDevice.req_block_location.get)
    assert("vod.asset" == tvDevice.req_block_location_name.get)
    assert("/collection/vo" == tvDevice.req_location.get)
    assert("/collection/vod.asset/query/dimension/er_lcn/in/18/dimension/type/eq/channel/dimension/is_epg_available/eq/1" == tvDevice.req_real_url.get)
    assert("/collection/vod.asset/query/dimension/er_lcn/in/18/dimension/type/eq/chann" == tvDevice.req_internal_url.get)
    assert(tvDevice.req_body.isEmpty)
    assert(tvDevice.req_body_byte_in.isEmpty)
    assert("5.165.116.29" == tvDevice.req_ip.get)
    assert(32596 == tvDevice.req_port.get)
    assert(tvDevice.req_user.isEmpty)
    assert(tvDevice.req_referer.isEmpty)
    assert("Opera/9.80 (Linux mips; Opera TV Store/6025; ce-html/1.0)" == tvDevice.req_agent.get)
    assert(tvDevice.req_forward.isEmpty)
    assert("HTTP/1.1" == tvDevice.req_protocol.get)
    assert(tvDevice.req_app_version.isEmpty)
    assert(tvDevice.resp_code.isEmpty)
    assert(5614 == tvDevice.resp_body_byte_sent.get)
    assert(0.0 - 1E-6 < tvDevice.resp_time.get && tvDevice.resp_time.get < 0.0 + 1E-6)
    assert(tvDevice.resp_upstream_addrs.isEmpty)
    assert( 0.0 - 1E-6 < tvDevice.resp_upstream_conn_time.get && tvDevice.resp_upstream_conn_time.get < 0.0 + 1E-6)
  }
}

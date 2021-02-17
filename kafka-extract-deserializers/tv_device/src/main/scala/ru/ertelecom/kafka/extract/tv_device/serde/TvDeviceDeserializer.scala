package ru.ertelecom.kafka.extract.tv_device.serde


import java.time.{Instant, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.tv_device.domain.TvDevice

import scala.collection.mutable

class TvDeviceDeserializer(appConf: Config) extends Deserializer(appConf) {

  lazy val ts_format = DateTimeFormatter.ISO_DATE_TIME
  lazy val req_time_format = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z").withLocale(Locale.US)

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    parse(payload)
  }

  def parse(payload: Array[Byte]): Option[TvDevice] = {
    val input = new String(payload)
    try {
      val kvMap =  new mutable.AnyRefMap[String, String]()

      input.split('\t').foreach(el => {
        val keyValueSplitted = el.split('=')
        val value: String = if (keyValueSplitted.length <= 1) null else keyValueSplitted(1)
        if (keyValueSplitted(0).contains("@timestamp")) {
          kvMap.put("ts", el.split('=')(1))
        } else if(value != null) {
          kvMap.put(keyValueSplitted(0).trim, value)
        }
      })
      val zonedTs = ZonedDateTime.parse(kvMap("ts"), ts_format)
      val tsLong = Instant.from(ts_format.parse(kvMap("ts"))).getEpochSecond

      Option.apply(TvDevice(
        ts = tsLong,
        server_name = kvMap("server.name"),
        server_point = kvMap("server.point"),
        server_type = kvMap("server.type"),
        server_num_conn = kvMap("server.number_connection").toLong,
        server_conn_req = kvMap.get("server.connection_requests").map(_.toInt),
        req_time = kvMap.get("request.time").map(s => Instant.from(req_time_format.parse(s)).getEpochSecond),
        req_schema = kvMap.get("request.sxema"),
        req_method = kvMap.get("request.metod"),
        req_host = kvMap.get("request.host"),
        req_block_location = kvMap.get("request.block_location"),
        req_block_location_name = kvMap.get("request.block_location_name"),
        req_location = kvMap.get("request.location"),
        req_real_url = kvMap.get("request.real_url"),
        req_internal_url = kvMap.get("request.internal_url"),
        req_body = kvMap.get("request.body"),
        req_body_byte_in = kvMap.get("request.body_byte_in"),
        req_ip = kvMap.get("request.ip"),
        req_port = kvMap.get("request.port").map(_.toInt),
        req_user = kvMap.get("request.user"),
        req_referer = kvMap.get("request.referer"),
        req_agent = kvMap.get("request.agent"),
        req_forward = kvMap.get("request.forward"),
        req_protocol = kvMap.get("request.protocol"),
        req_app_version = kvMap.get("request.x_app_version"),
        resp_code = kvMap.get("response.code").map(_.toInt),
        resp_body_byte_sent = kvMap.get("response.body_byte_sent").map(_.toInt),
        resp_time = kvMap.get("response.time").map(_.toDouble),
        resp_upstream_addrs = kvMap.get("response.upstream_addrs"),
        resp_upstream_conn_time = kvMap.get("response.upstream_connect_time").map(_.toDouble),
        resp_upstream_header_time = kvMap.get("response.upstream_header_time=([").map(_.toDouble),
        resp_upstream_resp_time = kvMap.get("response.upstream_response_time").map(_.toDouble),
        resp_upstream_status_code = kvMap.get("response.upstream_status_code").map(_.toInt),
        resp_cache_status = kvMap.get("response.cache_status=([").map(_.toInt),
        resp_cache_group = kvMap.get("response.cache_group"),
        resp_upstream_x_result_status = kvMap.get("response.upstream_x_result_status"),
        resp_upstream_tag_error = kvMap.get("response.upstream_tag_error"),
        resp_upstream_tag_error_code = kvMap.get("response.upstream_tag_error_code"),
        resp_lua_response = kvMap.get("response.lua_response").map(_.toInt),
        platform_id = kvMap.get("platform.id").map(_.toInt),
        platform_extid = kvMap.get("platform.extid"),
        subscriber_is_guest = kvMap.get("subscriber.is_guest").map(_.toInt != 0),
        subscriber_id = kvMap.get("subscriber.id").map(_.toInt),
        subscriber_extid = kvMap.get("subscriber.extid"),
        subscriber_token_type = kvMap.get("subscriber.token_type"),
        subscriber_token_is_expires = kvMap.get("subscriber.token_is_expires").map(_.toInt != 0),
        subscriber_operator_id = kvMap.get("subscriber.operator_id").map(_.toInt),
        subscriber_operator_extid = kvMap.get("subscriber.operator_extid"),
        subscriber_groups_title = kvMap.get("subscriber.groups_title"),
        subscriber_groups_id = kvMap.get("subscriber.groups_id"),
        device_id = kvMap.get("device.id"),
        device_extid = kvMap.get("device.extid"),
        year = zonedTs.getYear,
        month = zonedTs.getMonthValue,
        day = zonedTs.getDayOfMonth
      ))
    } catch {
      case e: Exception => Option.empty
    }
  }
}

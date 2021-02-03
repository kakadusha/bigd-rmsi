package ru.ertelecom.kafka.extract.sms.serde

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.sms.domain.SmsLogDelivery

class SmsLogDeliveryDeserializerTest extends FunSuite{
  test("test delivery") {
    val deserializer = new SmsLogDeliveryDeserializer(null)
    val json = """{"sms_query_id":10,"phone":"79173389889","status":"1","batch_id":"159","emulate":"1","time":"2019-09-21 14:18:12", "type": 1}"""
    val actual = deserializer.parseJson(json)
    val ts = System.currentTimeMillis() / 1000
    val expected = SmsLogDelivery(
      query_ts = 1569075492L,
      loaded_at = ts,
      sms_query_id = 10L,
      phone = "79173389889",
      status = 1,
      batch_id = 159,
      emulate = Option.apply(1),
      city_id = Option.empty,
      billing_id = Option.empty,
      `type` = Option.apply(1)
    )
    assert(actual == expected)
  }

  test("test delivery status is short") {
    val deserializer = new SmsLogDeliveryDeserializer(null)
    val json = """{"sms_query_id":10,"phone":"79173389889","status":1,"batch_id":"159","emulate":"1","time":"2019-09-21 14:18:12", "type": 1}"""
    val actual = deserializer.parseJson(json)
    val ts = System.currentTimeMillis() / 1000
    val expected = SmsLogDelivery(
      query_ts = 1569075492L,
      loaded_at = ts,
      sms_query_id = 10L,
      phone = "79173389889",
      status = 1,
      batch_id = 159,
      emulate = Option.apply(1),
      city_id = Option.empty,
      billing_id = Option.empty,
      `type` = Option.apply(1)
    )
    assert(actual == expected)
  }

  test("batch_id is null") {
    val deserializer = new SmsLogDeliveryDeserializer(null)
    val json = """{"sms_delivery_log_id":617583,"status":"1","time":"2020-11-11 13:31:15","phone":"78005553535","sms_query_id":2,"batch_id":null,"emulate":null,"city_billing_id":"1","billing_id":"1","type":null}"""
    val actual = deserializer.parseJson(json)
    val ts = System.currentTimeMillis() / 1000
    val expected = SmsLogDelivery(
      query_ts = 1605101475L,
      loaded_at = ts,
      sms_query_id = 2L,
      phone = "78005553535",
      status = 1,
      batch_id = 0,
      emulate = Option.empty,
      city_id = Option.apply(1),
      billing_id = Option.apply(1),
      `type` = Option.empty
    )
    assert(actual == expected)
  }
}

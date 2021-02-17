package ru.ertelecom.kafka.extract.sms.serde

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.sms.domain.SmsLogSend

class SmsLogSendDeserializerTest extends FunSuite {
  test("test simple json") {
    val json =
      """{
         "sms_query_id":1,
          "phone":"79173389889",
          "message":"3115 - \u0432\u0430\u0448 \u043a\u043e\u0434 (is your code for Wi-Fi)",
          "created_at":"2020-04-28 07:28:02",
          "batch_id":159,
          "emulate":1,
          "billing_id": 556,
          "city_id": 557
      }"""
    val deserializer = new SmsLogSendDeserializer(null)
    val actual = deserializer.parseJson(json)
    val expected = SmsLogSend(
      created_at = 1588058882,
      loaded_at = System.currentTimeMillis() / 1000,
      sms_query_id = 1,
      phone = "79173389889",
      msg_ru = "3115 - ваш код (is your code for Wi-Fi)",
      msg_en = null,
      msg_zh = null,
      batch_id = 159,
      emulate = Option.apply(1),
      billing_id = Option.apply(556),
      city_id = Option.apply(557),
      `type` = Option.empty
    )
    assert(actual.isDefined)
    assert(expected == actual.get)
  }

  test("test complex json") {
    val json =
      """{
          "sms_query_id": 1,
          "phone":"79173389889",
          "message":{
            "ru":"7715 - \u0432\u0430\u0448 \u043a\u043e\u0434 (is your code for Wi-Fi)",
            "en":"7715 - is your code for Wi-Fi",
            "zh":"7715 - is your code for Wi-Fi"
          },
          "created_at":"2020-04-21 11:03:08",
          "batch_id":159,
          "emulate":1,
          "billing_id":1,
          "type": 2
          }"""
    val deserializer = new SmsLogSendDeserializer(null)
    val actual = deserializer.parseJson(json)
    val expected = SmsLogSend(
      created_at = 1587466988,
      loaded_at = System.currentTimeMillis() / 1000,
      sms_query_id = 1,
      phone = "79173389889",
      msg_ru = "7715 - ваш код (is your code for Wi-Fi)",
      msg_en = "7715 - is your code for Wi-Fi",
      msg_zh = "7715 - is your code for Wi-Fi",
      batch_id = 159,
      emulate = Option.apply(1),
      billing_id = Option.apply(1),
      city_id = Option.empty,
      `type` = Option.apply(2)
    )
    assert(actual.isDefined)
    assert(expected == actual.get)
  }

  test("test empty query id") {
    val json =
      """{
          "sms_query_id": null,
          "phone":"79173389889",
          "message":{
            "ru":"7715 - \u0432\u0430\u0448 \u043a\u043e\u0434 (is your code for Wi-Fi)",
            "en":"7715 - is your code for Wi-Fi",
            "zh":"7715 - is your code for Wi-Fi"
          },
          "created_at":"2020-04-21 11:03:08"}"""
    val deserializer = new SmsLogSendDeserializer(null)
    val actual = deserializer.parseJson(json)
    assert(actual.isEmpty)
  }
}

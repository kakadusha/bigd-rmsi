package ru.ertelecom.kafka.extract.sms.serde

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import play.api.libs.json.{JsNumber, JsString, Json}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.sms.domain.SmsLogDelivery
import ru.ertelecom.kafka.extract.sms.util.ParseUtils

class SmsLogDeliveryDeserializer(appConf: Config) extends Deserializer(appConf) {
  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    parseJson(new String(payload))
  }

  def parseJson(payload: String): SmsLogDelivery = {
    val jsVal = Json.parse(payload)
    val qTs = LocalDateTime.parse(
      (jsVal \ "time").get
        .as[String]
        .replace(" ", "T")
    ).toEpochSecond(ZoneOffset.UTC)
    var cityId: Option[Int] = ParseUtils.parseBid(jsVal \ "city_billing_id")

    if(cityId.isEmpty) {
      cityId = ParseUtils.parseBid(jsVal \ "city_id")
    }

    SmsLogDelivery(
      query_ts = qTs,
      loaded_at = System.currentTimeMillis() / 1000,
      sms_query_id = (jsVal \ "sms_query_id").get.as[Long],
      phone = (jsVal \ "phone").get.as[String],
      status = (jsVal \ "status").get match {
        case JsString(value) => value.toShort
        case JsNumber(value) => value.toShortExact
        case _ => -1
      },
      batch_id = (jsVal \ "batch_id").get match {
        case JsString(value) => value.toLong
        case JsNumber(value) => value.toLongExact
        case _ => 0L
      },
      emulate = (jsVal \ "emulate").get match {
        case JsString(value) => Some(value.toInt)
        case JsNumber(value) => Some(value.toIntExact)
        case _ => None
      },
      city_id = cityId,
      billing_id = ParseUtils.parseBid(jsVal \ "billing_id"),
      `type`= (jsVal \ "type").asOpt[Int]
    )
  }
}

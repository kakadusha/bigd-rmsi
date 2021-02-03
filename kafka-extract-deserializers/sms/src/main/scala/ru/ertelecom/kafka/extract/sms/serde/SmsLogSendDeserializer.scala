package ru.ertelecom.kafka.extract.sms.serde

import java.time.{LocalDateTime, ZoneOffset}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import play.api.libs.json.{JsError, JsNumber, JsObject, JsString, JsSuccess, Json}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.sms.domain.SmsLogSend

class SmsLogSendDeserializer(appConf: Config) extends Deserializer(appConf){
  override def deserialize(): UserDefinedFunction = udf{payload: Array[Byte] =>
    parseJson(new String(payload))
  }

  def parseJson(payload: String): Option[SmsLogSend] = {
    val jsVal = Json.parse(payload)
    val createdTs = LocalDateTime.parse(
      (jsVal \ "created_at").get
        .as[String]
        .replace(" ", "T")
    ).toEpochSecond(ZoneOffset.UTC)
    var cityId: Option[Int] = (jsVal \ "city_billing_id").asOpt[Int]

    if(cityId.isEmpty) {
      cityId = (jsVal \ "city_id").asOpt[Int]
    }

    val msg = (jsVal \ "message").get
    var msgRu: String = null
    var msgEn: String = null
    var msgZh: String = null
    if(msg.isInstanceOf[JsObject]) {
      val msgObj = msg.as[JsObject]
      msgRu = (msgObj \ "ru").get.as[String]
      msgEn = (msgObj \ "en").get.asOpt[String].orNull
      msgZh = (msgObj \ "zh").get.asOpt[String].orNull
    } else {
      msgRu = msg.as[String]
    }
    (jsVal \ "sms_query_id").get.asOpt[Long].flatMap(queryId => {
      Option.apply(SmsLogSend(
        created_at = createdTs,
        loaded_at = System.currentTimeMillis() / 1000,
        sms_query_id = queryId,
        phone = (jsVal \ "phone").get.as[String],
        msg_ru = msgRu,
        msg_en = msgEn,
        msg_zh = msgZh,
        batch_id = (jsVal \ "batch_id").get match {
          case JsString(value) => value.toLong
          case JsNumber(value) => value.toLongExact
          case _ => 0L
        },
        emulate = (jsVal \ "emulate").asOpt[Int],
        city_id = cityId,
        billing_id = (jsVal \ "billing_id").asOpt[Int],
        `type`= (jsVal \ "type").asOpt[Int]
      ))
    })
  }
}

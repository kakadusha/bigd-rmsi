package ru.ertelecom.kafka.extract.rmsi.serde

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import play.api.libs.json.{JsValue, Json}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.rmsi.domain.RmsiMetrics

import java.time.{LocalDateTime, ZoneOffset}



class RmsiMetricsDeserializer(appConfig: Config) extends Deserializer(appConfig) with Logging {

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    this.parseJson(payload)
  }

  def parseJson(payload: Array[Byte]): Option[RmsiMetrics] = {
    try {
      val jsonVal: JsValue = Json.parse(payload)
      val result = RmsiMetrics(
        (jsonVal \ "serviceName").as[String],
        (jsonVal \ "entityType").as[String],
        LocalDateTime.parse((jsonVal \ "createTime").as[String].replace(" ", "T")).toEpochSecond(ZoneOffset.UTC),
        LocalDateTime.parse((jsonVal \ "lastUpdate").as[String].replace(" ", "T")).toEpochSecond(ZoneOffset.UTC),
        (jsonVal \ "billing").as[String],
        (jsonVal \ "message").asOpt[String] match {
          case Some("null") => None
          case None => None
          case Some(x) => Some(x)
        },
        (jsonVal \ "errorMessage").asOpt[String] match {
          case Some("null") => None
          case None => None
          case Some(x) => Some(x)
        },
        (jsonVal \ "status").asOpt[String],
        (jsonVal \ "result").asOpt[String],
        (jsonVal \ "login").asOpt[String],
        (jsonVal \ "networkActionType").asOpt[String],
        (jsonVal \ "networkActionTypeName").asOpt[String],
        (jsonVal \ "identificationType").asOpt[String],
        (jsonVal \ "houseId").asOpt[String],
        (jsonVal \ "nodeId").asOpt[String],
        (jsonVal \ "switchId").asOpt[String],
        (jsonVal \ "elementId").asOpt[String],
        (jsonVal \ "resourceId").asOpt[String],
        (jsonVal \ "network").asOpt[String],
        (jsonVal \ "ports").asOpt[String],
        (jsonVal \ "parameters").asOpt[String],
        (jsonVal \ "operationType").asOpt[String],
        (jsonVal \ "realLogin").asOpt[String],
        (jsonVal \ "impersonatedLogin").asOpt[String],
        (jsonVal \ "errorHistory").asOpt[Array[String]],
        (jsonVal \ "orderId").asOpt[String] match {
          case Some("null") => None
          case None => None
          case Some(x) => Some(x)
        },
        (jsonVal \ "requestId").asOpt[String],
        (jsonVal \ "companyId").asOpt[String],
        (jsonVal \ "office").asOpt[String],
        (jsonVal \ "packageId").asOpt[String],
        (jsonVal \ "agreementNumber").asOpt[String],
        (jsonVal \ "timeSlot").asOpt[String],
        (jsonVal \ "comment").asOpt[String],
        (jsonVal \ "transferAction").asOpt[String],
        (jsonVal \ "accessPanelId").asOpt[String],
        (jsonVal \ "screenshotLink").asOpt[String],
        (jsonVal \ "territories").asOpt[String]
      )
      Option.apply(result)
    }
    catch {
      case e: Exception => Option.empty
    }
  }
}
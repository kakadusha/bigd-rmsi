package ru.ertelecom.kafka.extract.web_domru_billing_api.serde

import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.web_domru_billing_api.domain.WebDomruBillingApi

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale
import java.nio.charset.StandardCharsets

class WebDomruBillingApiDeserializer(appConfig: Config) extends Deserializer(appConfig) {
 
    lazy val simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
    lazy val yyyymmdd = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH)

    def toInt(s: Option[String]): Option[Int] = s match {
        case None => None
        case Some(v) if v.isEmpty  => None
        case Some(v) => if (v.forall(_.isDigit) || (v.startsWith("-") && v.drop(1).forall(_.isDigit)))
            Some(v.toInt)
        else
            None
    }

    def toDouble(s: Option[String]): Option[Double] = s match {
        case None => None
        case Some(v) if v.isEmpty  => None
        case Some(v) => if (v.forall(_.isDigit) || (v.startsWith("-") && v.drop(1).forall(_.isDigit)))
            Some(v.toDouble)
        else
            None
    }

    override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
        this.parseCsv(payload)
    }

    def parseCsv(payload: Array[Byte]): Option[WebDomruBillingApi] = {
        try {

            val log = new String(payload, StandardCharsets.UTF_8).trim().split("\" ").map(v => {
                val m = v.split("=", 2)
                m(0) -> m(1).replaceAll("\"","")
            }).toMap

            val input_ts = log.get("timestamp").get
            val date = simpleDateFormat.parse(input_ts)
            val ts = (date.getTime / 1000)
            val log_entry_date = yyyymmdd.format(date)

            var remote_addr: String = "";
            if (log.get("x_forwarded_for").get == "-")
                remote_addr = log.get("remote_addr").get
            else
                if (log.get("x_real_ip").get == "-")
                    remote_addr = log.get("x_forwarded_for").get
                else
                    remote_addr = log.get("x_real_ip").get

            Option.apply(
                WebDomruBillingApi(
                    log_entry_date = log_entry_date,
                    ts = ts,
                    remote_addr = remote_addr,
                    http_status = log.get("http_status").get.toInt,
                    http_user_agent = log.get("user_agent").get,
                    x_request_id = if (log.get("x_request_id").get == "-") Option.empty else log.get("x_request_id"),
                    request_time = log.get("request_time").get.toDouble,
                    response_time = if (log.get("response_time").get == "-") Option.empty else toDouble(log.get("response_time")),
                    upstream_addr = if (log.get("upstream_addr").get == "-") Option.empty else log.get("upstream_addr"),
                    apistatus = if (log.get("apistatus").get == "-") Option.empty else toInt(log.get("apistatus")),
                    platform = if (log.get("platform").get == "-") Option.empty else log.get("platform"),
                    apiname = if (log.get("apiname").get == "-") Option.empty else log.get("apiname"),
                    api_errorcode = if (log.get("api_errorcode").get == "-") Option.empty else toInt(log.get("api_errorcode")),
                    agreementnumber = if (log.get("agreementnumber").get == "-") Option.empty else log.get("agreementnumber"),
                    args = if (log.get("args").get == "-") Option.empty else log.get("args"),
                    pkg = log.get("pkg").get,
                    city = log.get("city").get,
                    oauthclient = if (log.get("oauthclient").get == "-") Option.empty else log.get("oauthclient"),
                    system = log.get("system").get
                )
            )
        }
        catch {
            case e: Exception => Option.empty
        }
    }
}


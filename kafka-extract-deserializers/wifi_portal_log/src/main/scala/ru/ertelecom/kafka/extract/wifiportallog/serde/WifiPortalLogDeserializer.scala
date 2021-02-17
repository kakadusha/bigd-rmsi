package ru.ertelecom.kafka.extract.wifiportallog.serde

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.matching.Regex
import play.api.libs.json.{JsArray, JsObject, JsPath, JsString, JsValue, Json}
import ru.ertelecom.udf.funcs.cities.CitiesDict
import ru.ertelecom.kafka.extract.wifiportallog.domain.{WifiPortalLog, WifiPortalLogSource}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer

class WifiPortalLogDeserializer(appConfig: Config) extends Deserializer(appConfig) with Logging {

    final val MAC_REGEX = "^([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})[:.-]?([a-zA-Z0-9]{2})$"
    final val EX_CODE_REGEX_1 = "^.* (\\d{1,3}): .*$"
    final val EX_CODE_REGEX_2 = "^.*: (\\d{1,3})$"
    
    override def deserialize(): UserDefinedFunction = udf { payload: String =>
        this.parseJson(payload)
    }

    lazy val dict = new CitiesDict

    def reformat_mac(raw_mac: String): StringBuilder = {
        val mac = new StringBuilder()
        val keyValPattern = new Regex(MAC_REGEX)
        keyValPattern.findAllIn(raw_mac.toUpperCase()).matchData foreach { m => 
            mac.append(m.group(1) + m.group(2) + m.group(3) + m.group(4) + m.group(5) + m.group(6))
        }
        return mac
    }

    def get_except_code(message: String): Int = {
        val keyValPattern1 = new Regex(EX_CODE_REGEX_1)
        val keyValPattern2 = new Regex(EX_CODE_REGEX_2)
        var except_code = 0

        keyValPattern1.findAllIn(message).matchData foreach { m => 
            except_code = m.group(1).toInt
        }
        if (except_code == 0)
            keyValPattern2.findAllIn(message).matchData foreach { m => 
                except_code = m.group(1).toInt
            }
        return except_code
    }

    def toInt(s: Option[String]): Option[Int] = s match {
        case None => None
        case Some(v) if v.isEmpty  => None
        case Some(v) => if (v.forall(_.isDigit) || (v.startsWith("-") && v.drop(1).forall(_.isDigit)))
            Some(v.toInt)
        else
            None
    }

    def ipToLong(ip: String): Long = {
        val arr: Array[String] = ip.split("\\.")
        var num: Long = 0
        var i: Int = 0
        while (i < arr.length) {
        val power: Int = 3 - i
        num = num + ((arr(i).toInt % 256) * Math.pow(256, power)).toLong
        i += 1
        }
        num
    }

    def parseJson(payload_string: String): Option[WifiPortalLog] = {
        try {
            val wifiPortalLogOption:WifiPortalLogSource = Json.parse(payload_string).as[WifiPortalLogSource]

            val ip = wifiPortalLogOption.ip
              .map(ip => ipToLong(ip).asInstanceOf[scala.Long])

            val vlan:Option[String] = wifiPortalLogOption.vlan match {
                case Some(v) if !v.isEmpty => Some(v.toString().replace(".",":"))
                case _ => wifiPortalLogOption.location_id match {
                    case Some(q) if !q.isEmpty => Some(q.replace(".",":"))
                    case _ => Option.empty
                }
            }
            
            val city_id = 
                if(wifiPortalLogOption.city_id_domru != None) 
                    wifiPortalLogOption.city_id_domru
                else if (wifiPortalLogOption.city_id_tags != None) 
                    Some(dict.findByAlias(wifiPortalLogOption.city_id_tags.get).getCityId)
                else if (wifiPortalLogOption.city_id_user != None) 
                    Some(dict.findByAlias(wifiPortalLogOption.city_id_user.get).getCityId)
                else Some(-9999)
            
            val nas_ip = wifiPortalLogOption.nas_ip
              .map(nas_ip => ipToLong(nas_ip).asInstanceOf[scala.Long])

            var timeout = None: Option[Int]
            var filter = None: Option[String]
            var profile = None: Option[String]
            val radius_array = wifiPortalLogOption.radius_attrs
            if (!radius_array.isEmpty) {
                radius_array.get.foreach { it: Map[String, String] =>
                    if (it("attr_name") == "Alc-Subscr-Filter")
                        filter = it.get("attr_val")
                    if (it("attr_name") == "Alc-SLA-Prof-Str")
                        profile = it.get("attr_val")
                    if (it("attr_name") == "Session-Timeout")
                        timeout = toInt(it.get("attr_val"))
                }
            }

            val mac = wifiPortalLogOption.mac

            val domain_tags = wifiPortalLogOption.domain_tags
            val domain_user = wifiPortalLogOption.domain_user
            var domain:Option[String] = Option.empty
            if (domain_tags == null || domain_tags.isEmpty) {
                if (domain_user != null || !domain_user.isEmpty) {
                    if (domain_user != Some("alcatel")) {
                        domain = domain_user
                    }
                }
            } else {
                domain = domain_tags
            }

            Option.apply(
                WifiPortalLog(
                    ts = wifiPortalLogOption.ts,
                    except_message = wifiPortalLogOption.except_message,
                    except_code = get_except_code(wifiPortalLogOption.except_message),
                    except_name = wifiPortalLogOption.except_name,
                    http_code = wifiPortalLogOption.http_code,
                    endpoint = wifiPortalLogOption.endpoint,
                    mac = if (mac.isEmpty) Option.empty else Option.apply(reformat_mac(mac.get).toString),
                    ip = ip,
                    vlan = vlan,
                    domain = domain,
                    session = wifiPortalLogOption.session,
                    city_id = city_id,
                    page_id = wifiPortalLogOption.page_id,
                    user_name = wifiPortalLogOption.user_name,
                    nas_ip = nas_ip,
                    filter = filter,
                    profile = profile,
                    timeout = timeout
                )
            )
        } catch {
            case e: Exception => Option.empty
        }
    }
}


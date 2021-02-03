package ru.ertelecom.kafka.extract.wifiportallog.serde

import org.scalatest.FunSuite
import play.api.libs.json.{JsObject, Json}
import ru.ertelecom.kafka.extract.wifiportallog.domain.{WifiPortalLog, WifiPortalLogSource}

class WifiPortalLogDeserializerTest extends FunSuite{

   test("WifiPortalLogSource TEST") {
      val payload =
         """
   {
      "exeptionMessage":"\u041d\u0435\u0432\u043e\u0437\u043c\u043e\u0436\u043d\u043e \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441 \"index\/guesttttt\".",
      "timestamp":1584600991,
      "tags":{
         "name":"CHttpException",
         "code":404,
         "endPoint":"\/srv\/www\/protected\/vendor\/yiisoft\/yii\/framework\/web\/CWebApplication.php::286",
         "mac":"9cd64332643d",
         "ip":"10.0.1.42",
         "vlan":"101.3605",
         "domain":null
      },
      "user":{
         "id":"9cd64332643d",
         "hash":"1ffa7d224034beab7568daa44f2e3e4b",
         "vlan":"101.3605",
         "mac":"9cd64332643d",
         "router":"alcatel",
         "key":"ACEBE5000024FD5E731721",
         "nas_ip":"109.194.144.53",
         "nas_port":"3799",
         "nas_secret":"f",
         "nas_port_id":"lag-3:101.3605",
         "router_ip":"212.33.225.90",
         "radius_attrs":[
            {
               "attr_name":"Alc-Subscr-Filter",
               "attr_val":"Ingr-v4:120"
            },
            {
               "attr_name":"Alc-SLA-Prof-Str",
               "attr_val":"SLA-WIFI-MAX"
            }
         ],
         "domain":"alcatel",
         "ip":"10.0.1.42",
         "acct_session_id":"ACEBE5000024FD5E731721",
         "DomRuPixel":{
            "AcctId":"ACEBE5000024FD5E731721",
            "CityId":4,
            "DeviceIP":"10.0.1.42",
            "DeviceMac":"9cd64332643d",
            "FbId":null,
            "InstId":null,
            "LocationId":"101.3605",
            "LoginId":null,
            "OkId":null,
            "PageId":"\/guest",
            "RouterId":"alcatel",
            "Timestamp":"1584600977",
            "TwId":null,
            "UserId":null,
            "VkId":null
         }
      },
      "fingerprint":[
         "{{ default }}",
         "ecb0b4bd8e4c289232804fbb013d77e7"
      ]
   }
   """.stripMargin
      val deserializer = new WifiPortalLogDeserializer(null)
      val wifiPortalLogOption:WifiPortalLogSource = Json.parse(payload).as[WifiPortalLogSource]

      assert(1584600991L == wifiPortalLogOption.ts)
      wifiPortalLogOption.radius_attrs
      assert(Some(List(Map("attr_name" -> "Alc-Subscr-Filter", "attr_val" -> "Ingr-v4:120"), Map("attr_name" -> "Alc-SLA-Prof-Str", "attr_val" -> "SLA-WIFI-MAX"))) == wifiPortalLogOption.radius_attrs)
   }

   test("WifiPortalLogSource TEST empty") {
      val payload =
         """
      {
         "exeptionMessage":"\u041e\u0448\u0438\u0431\u043a\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f 216: \u043d\u0435 \u043f\u0435\u0440\u0435\u0434\u0430\u043d \u0438\u0434\u0435\u043d\u0442\u0438\u0444\u0438\u043a\u0430\u0442\u043e\u0440 PHP-\u0441\u0435\u0441\u0441\u0438\u0438",
         "timestamp":1585223064,
         "tags":{
            "name":"application\\exceptions\\BadRequestException",
            "code":401,
            "endPoint":"\/srv\/www\/protected\/modules\/index\/controllers\/IndexController.php::54",
            "mac":null,
            "ip":null,
            "vlan":null,
            "domain":null
         },
         "user":{
            "id":null
         },
         "fingerprint":[
            "{{ default }}",
            "eda3f8521553d40508a2b604014fc34f"
         ]
      }
   """.stripMargin
      val deserializer = new WifiPortalLogDeserializer(null)
      val wifiPortalLogOption:WifiPortalLogSource = Json.parse(payload).as[WifiPortalLogSource]

      assert(1585223064L == wifiPortalLogOption.ts)
      wifiPortalLogOption.radius_attrs
      assert(None == wifiPortalLogOption.radius_attrs)
   }

   test("successWifiPortalLogDeserializerJson1Test") {
      val payload =
         """
   {
      "exeptionMessage":"\u041d\u0435\u0432\u043e\u0437\u043c\u043e\u0436\u043d\u043e \u043e\u0431\u0440\u0430\u0431\u043e\u0442\u0430\u0442\u044c \u0437\u0430\u043f\u0440\u043e\u0441 \"index\/guesttttt\".",
      "timestamp":1584600991,
      "tags":{
         "name":"CHttpException",
         "code":404,
         "endPoint":"\/srv\/www\/protected\/vendor\/yiisoft\/yii\/framework\/web\/CWebApplication.php::286",
         "mac":"9cd64332643d",
         "ip":"10.0.1.42",
         "vlan":"101.3605",
         "domain":null
      },
      "user":{
         "id":"9cd64332643d",
         "hash":"1ffa7d224034beab7568daa44f2e3e4b",
         "vlan":"101.3605",
         "mac":"9cd64332643d",
         "router":"alcatel",
         "key":"ACEBE5000024FD5E731721",
         "nas_ip":"109.194.144.53",
         "nas_port":"3799",
         "nas_secret":"f",
         "nas_port_id":"lag-3:101.3605",
         "router_ip":"212.33.225.90",
         "radius_attrs":[
            {
               "attr_name":"Alc-Subscr-Filter",
               "attr_val":"Ingr-v4:120"
            },
            {
               "attr_name":"Alc-SLA-Prof-Str",
               "attr_val":"SLA-WIFI-MAX"
            }
         ],
         "domain":"alcatel",
         "ip":"10.0.1.42",
         "acct_session_id":"ACEBE5000024FD5E731721",
         "DomRuPixel":{
            "AcctId":"ACEBE5000024FD5E731721",
            "CityId":4,
            "DeviceIP":"10.0.1.42",
            "DeviceMac":"9cd64332643d",
            "FbId":null,
            "InstId":null,
            "LocationId":"101.3605",
            "LoginId":null,
            "OkId":null,
            "PageId":"\/guest",
            "RouterId":"alcatel",
            "Timestamp":"1584600977",
            "TwId":null,
            "UserId":null,
            "VkId":null
         }
      },
      "fingerprint":[
         "{{ default }}",
         "ecb0b4bd8e4c289232804fbb013d77e7"
      ]
   }
   """.stripMargin
      val deserializer = new WifiPortalLogDeserializer(null)
      val wifiPortalLogOption = deserializer.parseJson(payload)

      assert(wifiPortalLogOption.isDefined)

      val wifiPortalLog = wifiPortalLogOption.get
      assert(1584600991 == wifiPortalLog.ts)
      assert("Невозможно обработать запрос \"index/guesttttt\"." == wifiPortalLog.except_message)
      assert(0 == wifiPortalLog.except_code)
      assert("CHttpException" == wifiPortalLog.except_name)
      assert(Option.apply(404) == wifiPortalLog.http_code)
      assert("/srv/www/protected/vendor/yiisoft/yii/framework/web/CWebApplication.php::286" == wifiPortalLog.endpoint)
      assert(Option.apply("9CD64332643D") == wifiPortalLog.mac)
      assert(Option.apply(167772458) == wifiPortalLog.ip)
      assert(Option.apply("101:3605") == wifiPortalLog.vlan)
      assert(None == wifiPortalLog.domain)
      assert(Option.apply("ACEBE5000024FD5E731721") == wifiPortalLog.session)
      assert(Some(4) == wifiPortalLog.city_id)
      assert(Option.apply("/guest") == wifiPortalLog.page_id)
      assert(None == wifiPortalLog.user_name)
      assert(Option.apply(1841467445) == wifiPortalLog.nas_ip)
      assert(Option.apply("Ingr-v4:120") == wifiPortalLog.filter)
      assert(Option.apply("SLA-WIFI-MAX") == wifiPortalLog.profile)
      assert(None == wifiPortalLog.timeout)
   }

  test("successWifiPortalLogDeserializerJson2Test") {
      val payload =
      """
      {
         "exeptionMessage":"\u041e\u0448\u0438\u0431\u043a\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f 216: \u043d\u0435 \u043f\u0435\u0440\u0435\u0434\u0430\u043d \u0438\u0434\u0435\u043d\u0442\u0438\u0444\u0438\u043a\u0430\u0442\u043e\u0440 PHP-\u0441\u0435\u0441\u0441\u0438\u0438",
         "timestamp":1585223064,
         "tags":{
            "name":"application\\exceptions\\BadRequestException",
            "code":401,
            "endPoint":"\/srv\/www\/protected\/modules\/index\/controllers\/IndexController.php::54",
            "mac":null,
            "ip":null,
            "vlan":null,
            "domain":null
         },
         "user":{
            "id":null
         },
         "fingerprint":[
            "{{ default }}",
            "eda3f8521553d40508a2b604014fc34f"
         ]
      }
      """.stripMargin

      val deserializer = new WifiPortalLogDeserializer(null)

      val raw_json = Json.parse(payload).as[JsObject]
      assert(1585223064 == (raw_json \ "timestamp").as[Int])
      assert("\u041e\u0448\u0438\u0431\u043a\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f 216: \u043d\u0435 \u043f\u0435\u0440\u0435\u0434\u0430\u043d \u0438\u0434\u0435\u043d\u0442\u0438\u0444\u0438\u043a\u0430\u0442\u043e\u0440 PHP-\u0441\u0435\u0441\u0441\u0438\u0438" == (raw_json \ "exeptionMessage").as[String])
      assert(216 == deserializer.get_except_code((raw_json \ "exeptionMessage").as[String]))
      assert("application\\exceptions\\BadRequestException" == (raw_json \ "tags" \ "name").as[String])
      assert("/srv/www/protected/modules/index/controllers/IndexController.php::54" == (raw_json \ "tags" \ "endPoint").as[String])
      

      val wifiPortalLogOption = deserializer.parseJson(payload)
      assert(wifiPortalLogOption.isDefined)

      val wifiPortalLog = wifiPortalLogOption.get
      assert(1585223064 == wifiPortalLog.ts)
      assert("Ошибка подключения 216: не передан идентификатор PHP-сессии" == wifiPortalLog.except_message)
      assert(216 == wifiPortalLog.except_code)
      assert("application\\exceptions\\BadRequestException" == wifiPortalLog.except_name)
      assert(Option.apply(401) == wifiPortalLog.http_code)
      assert("/srv/www/protected/modules/index/controllers/IndexController.php::54" == wifiPortalLog.endpoint)
      assert(None == wifiPortalLog.mac)
      assert(None == wifiPortalLog.ip)
      assert(None == wifiPortalLog.vlan)
      assert(None == wifiPortalLog.domain)
      assert(None == wifiPortalLog.session)
      assert(Some(-9999) == wifiPortalLog.city_id)
      assert(None == wifiPortalLog.page_id)
      assert(None == wifiPortalLog.user_name)
      assert(None == wifiPortalLog.nas_ip)
      assert(None == wifiPortalLog.filter)
      assert(None == wifiPortalLog.profile)
      assert(None == wifiPortalLog.timeout)
   }

     test("successWifiPortalLogDeserializerJson BRYANSK ALCATEL") {
      val payload =
      """
{
   "exeptionMessage":"\u041e\u0448\u0438\u0431\u043a\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f 301: \u0412\u043d\u0443\u0442\u0440\u0435\u043d\u043d\u044f\u044f \u043e\u0448\u0438\u0431\u043a\u0430 \u0441\u0435\u0440\u0432\u0438\u0441\u0430: 301",
   "timestamp":1593096837,
   "tags":{
      "name":"application\\behaviors\\RouterConsoleException",
      "code":401,
      "endPoint":"\/srv\/www\/protected\/behaviors\/RouterStatusBehavior.php::84",
      "mac":"1819d6910cfe",
      "ip":"10.13.249.163",
      "vlan":null,
      "domain":null
   },
   "user":{
      "id":"1819d6910cfe",
      "hash":"4a434a18c7935737a599b92fac276d46",
      "mac":"1819d6910cfe",
      "router":"alcatel",
      "key":"EFA1D413F9E2395EF49D4F",
      "nas_ip":"109.194.0.76",
      "nas_port":"3799",
      "nas_secret":"LfqntRjirtCkjdj",
      "nas_port_id":"lag-13:1197.3605",
      "router_ip":"109.194.0.161",
      "radius_attrs":[
         {
            "attr_name":"Alc-SLA-Prof-Str",
            "attr_val":"SLA_P7L2N176"
         },
         {
            "attr_name":"User-Name",
            "attr_val":"89155363365@wifi"
         }
      ],
      "domain":"bryansk",
      "ip":"10.13.249.163",
      "acct_session_id":"EFA1D413F9E2395EF49D4F",
      "DomRuPixel":{
         "AcctId":"EFA1D413F9E2395EF49D4F",
         "CityId":null,
         "DeviceIP":"10.13.249.163",
         "DeviceMac":"1819d6910cfe",
         "FbId":null,
         "InstId":null,
         "Language":"ru",
         "LocationId":"1197.3605",
         "LoginId":"9155363365",
         "OkId":null,
         "PageId":"\/guest\/marketview",
         "RouterId":"alcatel",
         "Timestamp":"1593089374",
         "TwId":null,
         "UserId":"89155363365@wifi",
         "VkId":null
      },
      "request_uri":"\/guest\/marketview?dst=http%3A%2F%2Fbryansk.domru.wi-fi.ru%2F"
   },
   "fingerprint":[
      "{{ default }}",
      "a68f2d3448b322e82fcf216571927e9b"
   ]
}
      """.stripMargin

      val deserializer = new WifiPortalLogDeserializer(null)     
      val wifiPortalLogOption = deserializer.parseJson(payload)
      assert(wifiPortalLogOption.isDefined)

      val wifiPortalLog = wifiPortalLogOption.get
      assert(Some("bryansk") == wifiPortalLog.domain)
      assert(Some("1197:3605") == wifiPortalLog.vlan)
   }
}

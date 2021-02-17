package ru.ertelecom.kafka.extract.wifi_accnt.serde

import org.scalatest.FunSuite

import ru.ertelecom.kafka.extract.wifi_accnt.domain.WifiAccnt

class WifiAccntDeserializerTestSuite extends FunSuite {
  test("test: wifisub unauthorized") {
    val deserializer = new WifiAccntDeserializer(null)
    val input =
      """{"un":"ec:1f:72:3c:e0:c3","nasip":"109.195.80.76","nasid":"bsr01-spb","astype":2,"asid":"F8B30108DD79045F34C5F7","aioct":101027,"aigw":0,"aooct":1413409,"aogw":0,"callstationid":"ec:1f:72:3c:e0:c3","framedip":"10.13.94.93","astime":1800,"eventts":1597295873,"adslacid":"bsr01-spb|120|LAG-CORE|lag-1:752.3610","alcslaprof":"SLA-REDIRECT","atcause":5,"amsessionid":"F8B30108DD791F5F34C5F9","nasportid":"lag-1:752.3610","hwidname":"someHwIdnm","hwipname":"someHwIpnm","alcclienthw":"ec:1f:72:3c:e0:c3","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":0,"alcaiallpackets1":0,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":0,"alcaoallpackets1":0,"eqts":1597295873,"wifisub":1,"bid":556,"nasudpport":64427,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
    val record = deserializer.parseJson(input)
    assert(record.isDefined)
    val expected_rec = Option(WifiAccnt("ec:1f:72:3c:e0:c3", "bsr01-spb", 2, "F8B30108DD79045F34C5F7", 101027L, 0L, 1413409L,
      0L, Some("ec:1f:72:3c:e0:c3"), "10.13.94.93", 1800L, 1597295873L, Option("bsr01-spb|120|LAG-CORE|lag-1:752.3610"), Option("SLA-REDIRECT"),
      5, Option("F8B30108DD791F5F34C5F9"), Some("lag-1:752.3610"), "109.195.80.76", "15", Option("someHwIdnm"), Option("someHwIpnm"),
      Option("ec:1f:72:3c:e0:c3"), 1, 1, "752:3610", 556, 1800L, 101027L, 1413409L, 1597294073L, 1597295873L))
    assert(record == expected_rec)
  }
   test("test: cache") {
     val deserializer = new WifiAccntDeserializer(null)
     val input =
       """{"un":"ec:1f:72:3c:e0:c3","nasip":"109.195.80.76","nasid":"bsr01-spb","astype":2,"asid":"F8B30108DD79045F34C5F7","aioct":101027,"aigw":0,"aooct":1413409,"aogw":0,"callstationid":"ec:1f:72:3c:e0:c3","framedip":"10.13.94.93","astime":1800,"eventts":1597295873,"adslacid":"bsr01-spb|120|LAG-CORE|lag-1:752.3610","alcslaprof":"SLA-REDIRECT","atcause":5,"amsessionid":"F8B30108DD791F5F34C5F9","nasportid":"lag-1:752.3610","hwidname":"someHwIdnm","hwipname":"someHwIpnm","alcclienthw":"ec:1f:72:3c:e0:c3","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":0,"alcaiallpackets1":0,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":0,"alcaoallpackets1":0,"eqts":1597295873,"wifisub":1,"bid":556,"nasudpport":64427,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
     val almost_same_input =
       """{"un":"ec:1f:72:3c:e0:c3","nasip":"109.195.80.76","nasid":"bsr01-spb","astype":2,"asid":"F8B30108DD79045F34C5F7","aioct":101027,"aigw":0,"aooct":1413409,"aogw":0,"callstationid":"ec:1f:72:3c:e0:c3","framedip":"10.13.94.93","astime":1800,"eventts":1597296873,"adslacid":"bsr01-spb|120|LAG-CORE|lag-1:752.3610","alcslaprof":"SLA-REDIRECT","atcause":5,"amsessionid":"F8B30108DD791F5F34C5F9","nasportid":"lag-1:752.3610","hwidname":"someHwIdnm","hwipname":"someHwIpnm","alcclienthw":"ec:1f:72:3c:e0:c3","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":0,"alcaiallpackets1":0,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":0,"alcaoallpackets1":0,"eqts":1597295873,"wifisub":1,"bid":556,"nasudpport":64427,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
     val record = deserializer.parseJson(input)
     val record_duplicate = deserializer.parseJson(input)
     val almost_same_record = deserializer.parseJson(almost_same_input)
     assert(record.isDefined)
     assert(record_duplicate.isEmpty)
     assert(almost_same_record.isDefined)
   }
 test("test: wifisub guest"){
   val deserializer = new WifiAccntDeserializer(null)
   val input = """{"un":"89120598105@wifi","nasip":"212.33.234.76","nasid":"bsr01-perm","astype":2,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":0,"aooct":174062088,"aogw":0,"callstationid":"74:23:44:55:19:9e","framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"pw-3077:3601","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":1,"bid":1,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
   val record = deserializer.parseJson(input)
   val expected_rec = Option(WifiAccnt("89120598105@wifi", "bsr01-perm", 2, "1B1003217666D35F34C155", 2627769L, 0L,
   174062088L, 0L, Some("74:23:44:55:19:9e"), "10.12.13.151", 2984L, 1597295870L, Option.empty, Option("SLA-WIFI-GUEST"),
   0, Option("1B1003217871F95F34CA12"), Some("pw-3077:3601"), "212.33.234.76", "15", Option.empty, Option.empty, Option("74:23:44:55:19:9e"),
   1, 7, "3077:3601", 1, 2984L, 2627769L, 174062088L, 1597292886L, 1597295870L))
   assert(record == expected_rec)
 }
   test("test: wrong astype"){
     val deserializer = new WifiAccntDeserializer(null)
     val input = """{"un":"89120598105@wifi","nasip":"212.33.234.76","nasid":"bsr01-perm","astype":3,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":0,"aooct":174062088,"aogw":0,"callstationid":"74:23:44:55:19:9e","framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"pw-3077:3601","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":1,"bid":1,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
     val record = deserializer.parseJson(input)
     assert(record.isEmpty)
   }
  test("test: premium un with bid=7777"){
    val deserializer = new WifiAccntDeserializer(null)
    val input = """{"un":"89120598105@wifi_pa","nasip":"212.33.234.76","nasid":"bsr01-perm","astype":2,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":0,"aooct":174062088,"aogw":0,"callstationid":"74:23:44:55:19:9e","framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"slot=6;subslot=2;port=14;vlanid=3887;vlanid2=770;","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":0,"bid":7777,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
    val record = deserializer.parseJson(input)
    val expected_rec = Option(WifiAccnt("89120598105@wifi_pa", "bsr01-perm", 2, "1B1003217666D35F34C155", 2627769L,
    0L, 174062088L, 0L, Some("74:23:44:55:19:9e"), "10.12.13.151", 2984L, 1597295870L, Option.empty, Option("SLA-WIFI-GUEST"),
    0, Option("1B1003217871F95F34CA12"), Some("slot=6;subslot=2;port=14;vlanid=3887;vlanid2=770;"), "212.33.234.76",
    "15", Option.empty, Option.empty, Option("74:23:44:55:19:9e"), 0, 6, "3887:770", 7777, 2984L, 2627769L, 174062088L,
    1597292886L, 1597295870L))
    assert(record == expected_rec)
  }
  test("test: customer with gigawords"){
    val deserializer = new WifiAccntDeserializer(null)
    val input = """{"un":"v1337","nasip":"212.33.234.76","nasid":"bsr01-perm","astype":2,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":1,"aooct":174062088,"aogw":2,"callstationid":"74:23:44:55:19:9e","framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"bng02-spb eth 0/3/2/4:868.2421","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":0,"bid":7777,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
    val record = deserializer.parseJson(input)
    val expected_rec = Option(WifiAccnt("v1337", "bsr01-perm", 2, "1B1003217666D35F34C155", 2627769L, 1L, 174062088L,
    2L, Some("74:23:44:55:19:9e"), "10.12.13.151", 2984L, 1597295870L, Option.empty, Option("SLA-WIFI-GUEST"), 0,
    Option("1B1003217871F95F34CA12"), Some("bng02-spb eth 0/3/2/4:868.2421"), "212.33.234.76", "15", Option.empty,
    Option.empty, Option("74:23:44:55:19:9e"), 0, 5, "868:2421", 7777, 2984L, 4297595065L, 8763996680L, 1597292886L,
    1597295870L))
    assert(record == expected_rec)
  }
  test("test: weird un and digit nasportid"){
    val deserializer = new WifiAccntDeserializer(null)
    val input = """{"un":"c46e:1f88:7139","nasip":"212.33.234.76","nasid":"bsr01-perm","astype":2,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":1,"aooct":174062088,"aogw":2,"callstationid":"74:23:44:55:19:9e","framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"306.3649","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":0,"bid":7777,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
    val record = deserializer.parseJson(input)
    val expected_rec = Option(WifiAccnt("c4:6e:1f:88:71:39", "bsr01-perm", 2, "1B1003217666D35F34C155", 2627769L,
    1L, 174062088L, 2L, Some("74:23:44:55:19:9e"), "10.12.13.151", 2984L, 1597295870L, Option.empty, Option("SLA-WIFI-GUEST"),
    0, Option("1B1003217871F95F34CA12"), Some("306.3649"), "212.33.234.76", "15", Option.empty, Option.empty, Option("74:23:44:55:19:9e"),
    0, 5, "306:3649", 7777, 2984L, 4297595065L, 8763996680L, 1597292886L, 1597295870L))
    assert(record == expected_rec)
  }
  
   test("test: not wifi"){
     val deserializer = new WifiAccntDeserializer(null)
     val input = """{"un":"10.188.205.51","nasip":"109.195.81.41","nasid":"bng01-spb","astype":3,"asid":"bng01-s0527008360245211a96e049316","aioct":0,"aigw":0,"aooct":0,"aogw":0,"callstationid":"00:13:77:95:92:65","framedip":"10.188.205.51","astime":0,"eventts":1599224348,"adslacid":null,"alcslaprof":null,"atcause":0,"amsessionid":null,"nasportid":"bng01-spb eth 0/5/2/70:836.2452","hwidname":"ipoe","hwipname":null,"alcclienthw":null,"nasporttype":15,"eqts":1599224348,"wifisub":0,"bid":556,"nasudpport":1813,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
     val record = deserializer.parseJson(input)
     assert(record.isEmpty)
   }
  test("test: missing callstationid"){
    val deserializer = new WifiAccntDeserializer(null)
    val input_null = """{"un":"c46e:1f88:7139","nasip":"212.33.234.76","nasid":"bsr01-perm","astype":2,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":1,"aooct":174062088,"aogw":2,"callstationid":null,"framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"306.3649","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":0,"bid":7777,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
    val input_miss = """{"un":"c47e:1f88:7139","nasip":"212.33.234.76","nasid":"bsr01-perm","astype":2,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":1,"aooct":174062088,"aogw":2,"framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"306.3649","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":0,"bid":7777,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
    val record_null = deserializer.parseJson(input_null)
    val record_miss = deserializer.parseJson(input_miss)
    assert(record_null.isDefined)
    assert(record_miss.isDefined)
  }
  test("test: missing mandatory values"){
    val deserializer = new WifiAccntDeserializer(null)
    val input_null = """{"un":null,"nasip":"212.33.234.76","nasid":"bsr01-perm","astype":2,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":1,"aooct":174062088,"aogw":2,"callstationid":null,"framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"306.3649","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":0,"bid":7777,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
    val input_miss = """{"nasip":"212.33.234.76","nasid":"bsr01-perm","astype":2,"asid":"1B1003217666D35F34C155","aioct":2627769,"aigw":1,"aooct":174062088,"aogw":2,"framedip":"10.12.13.151","astime":2984,"eventts":1597295870,"adslacid":null,"alcslaprof":"SLA-WIFI-GUEST","atcause":0,"amsessionid":"1B1003217871F95F34CA12","nasportid":"306.3649","hwidname":null,"hwipname":null,"alcclienthw":"74:23:44:55:19:9e","nasporttype":15,"alcaistatmode1":"0x8001 minimal","alcaialloctets1":877554,"alcaiallpackets1":6054,"alcaistatmode4":"0x8004 minimal","alcaialloctets4":1735434,"alcaiallpackets4":13088,"alcaostatmode1":"0x8001 minimal","alcaoalloctets1":139167150,"alcaoallpackets1":101709,"alcaostatmode4":"0x8004 minimal","alcaoalloctets4":34860769,"alcaoallpackets4":25217,"eqts":1597295870,"wifisub":0,"bid":7777,"nasudpport":64479,"adslarid":null,"ruckusssid":null}""".stripMargin.getBytes
    val record_null = deserializer.parseJson(input_null)
    val record_miss = deserializer.parseJson(input_miss)
    assert(record_null.isEmpty)
    assert(record_miss.isEmpty)
  }
}

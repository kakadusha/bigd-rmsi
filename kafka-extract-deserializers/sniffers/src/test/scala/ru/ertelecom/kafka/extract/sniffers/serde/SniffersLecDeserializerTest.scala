package ru.ertelecom.kafka.extract.sniffers.serde
import java.nio.charset.StandardCharsets

import org.scalatest.FunSuite
class SniffersLecDeserializerTest extends FunSuite {
  test("successSniffersLecDeserializerTest") {
    val payload =
      """2019-10-15T11:39:49+00:00	109.194.155.9	/wifi/monitor	{ "col": [ [ "48:2C:A0:96:42:5F", -67, 11, "1570616202.210" ]], "mac": "e4:8d:8c:c8:87:17"}""".stripMargin.getBytes
    val deserializer = new SniffersLecDeserializer(null)
    val sniffersLecOption = deserializer.parseJson(payload)
    println(sniffersLecOption)
    assert(sniffersLecOption.isDefined)
    val SniffersLec = sniffersLecOption.get
    assert(1 == SniffersLec.length)
    assert(1570616202L == SniffersLec(0).ts)
    assert("E48D8CC88717" == SniffersLec(0).sensor)
    assert(1841470217L == SniffersLec(0).ip)
    assert("482CA096425F" == SniffersLec(0).mac)
    assert(11 == SniffersLec(0).channel)
  }

}

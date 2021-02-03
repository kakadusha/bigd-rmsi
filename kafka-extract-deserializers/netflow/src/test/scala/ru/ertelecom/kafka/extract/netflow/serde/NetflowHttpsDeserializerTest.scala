package ru.ertelecom.kafka.extract.netflow.serde

import org.scalatest.FunSuite

class NetflowHttpsDeserializerTest extends FunSuite{
  test("testLoginIsNull") {
    val deserializer = new NetflowHttpsDeserializer(null)
    val testValue = "552\t1573723723\t5.44.53.133\t-\tds1.mirconnect.ru\t1979877867"
    val netflowRecord = deserializer.parseTsv(testValue.getBytes("UTF-8"))
    assert(netflowRecord.isDefined)
    assert(552 == netflowRecord.get.bid)
    assert(1573723723L == netflowRecord.get.ts)
    assert(86783365L == netflowRecord.get.wip)
    assert(netflowRecord.get.cid.isEmpty)
    assert("ds1.mirconnect.ru" == netflowRecord.get.sni)
    assert("1979877867" == netflowRecord.get.cip)
    assert("2019-11-14" == netflowRecord.get.forDay)
  }
}

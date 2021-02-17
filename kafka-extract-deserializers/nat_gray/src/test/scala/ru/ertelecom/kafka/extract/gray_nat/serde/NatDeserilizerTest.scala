package ru.ertelecom.kafka.extract.gray_nat.serde

import java.nio.charset.Charset

import org.scalatest.FunSuite

class NatDeserilizerTest extends FunSuite{
  test("testLoginIsNull") {
    val deserializer = new NatDeserializer(null)
    val testValue = "556\t-\t1\t10.15.78.106\t109.195.90.14\t65032\t65283"
    val nat = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))

    assert(nat.isDefined)
    assert(nat.get.billingId == 556)
    assert(nat.get.login.isEmpty)
    assert(nat.get.`type` == 1)
    assert(nat.get.grayIp == 168775274L)
    assert(nat.get.whiteIp == 1841519118L)
    assert(nat.get.startPort == 65032)
    assert(nat.get.endPort == 65283)
  }

  test("testInvalidTsv") {
    val deserializer = new NatDeserializer(null)
    val testValue = "556\t-\t1\t109.195.90.14\t65032\t65283"
    val nat = deserializer.parseTsv(testValue.getBytes(Charset.forName("UTF-8")))
    assert(nat.isEmpty)
  }
}

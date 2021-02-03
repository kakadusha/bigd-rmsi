package ru.ertelecom.kafka.extract.rmsi.serde

import org.scalatest.FunSuite


class RmsiMetricsDeserializerTest  extends FunSuite {

  def readSampleFromFile(name: String): List[String] = {
    val source = scala.io.Source.fromInputStream(getClass.getResource(name).openStream, "utf-8")
    try source.getLines.toList
    finally source.close()
  }

  test("successRmsiMetricsDeserializerTest") {
    val payload ="""
{"serviceName":"BPMS-SERVICE","entityType":"BPMS_REQUEST","createTime":"2020-12-16 07:17:55.527","lastUpdate":"2020-12-16 07:17:56.840","billing":"krsk","message":"{\"id\":\"2072.210761\",\"model\":\"Поддержка сервиса\",\"container\":\"PS\",\"version\":\"V1.4.5\",\"state\":\"running\",\"container_name\":\"Поддержка Сервиса\",\"version_name\":\"V1.4.5\",\"creation_time\":\"2020-12-16T07:17:56.811Z\",\"due_date\":\"2020-12-16T11:17:56.810Z\",\"modification_time\":\"2020-12-16T07:17:56.830Z\",\"branch_name\":\"V1.4.5\"}","status":"DONE","login":"bocharov.ra","parameters":"{phone=89233633096, orderId=order34441291, employee=bocharov.ra, billingUrl=krsk}","operationType":"bpmsRequest","orderId":"null"}
    """.stripMargin.getBytes
    val deserializer = new RmsiMetricsDeserializer(null)
    val RmsiMetricsOption = deserializer.parseJson(payload)
    assert(deserializer.parseJson(payload).isDefined)
    val RmsiMetrics = RmsiMetricsOption.get

    assert("BPMS-SERVICE" == RmsiMetrics.serviceName)
    assert("BPMS_REQUEST" == RmsiMetrics.entityType)
    assert(1608103075 == RmsiMetrics.createTime)
    assert(1608103076 == RmsiMetrics.lastUpdate)
    assert("krsk" == RmsiMetrics.billing)
    assert(Some("{\"id\":\"2072.210761\",\"model\":\"Поддержка сервиса\",\"container\":\"PS\",\"version\":\"V1.4.5\",\"state\":\"running\",\"container_name\":\"Поддержка Сервиса\",\"version_name\":\"V1.4.5\",\"creation_time\":\"2020-12-16T07:17:56.811Z\",\"due_date\":\"2020-12-16T11:17:56.810Z\",\"modification_time\":\"2020-12-16T07:17:56.830Z\",\"branch_name\":\"V1.4.5\"}") == RmsiMetrics.message)
    assert(Some("DONE") == RmsiMetrics.status)
    assert(Some("bocharov.ra") == RmsiMetrics.login)
    assert(Some("{phone=89233633096, orderId=order34441291, employee=bocharov.ra, billingUrl=krsk}") == RmsiMetrics.parameters)
    assert(Some("bpmsRequest") == RmsiMetrics.operationType)
    assert(Some("null") == RmsiMetrics.orderId)
  }

  test("successRmsiMetricsDeserializerTestMultiple") {
    val samplesList = readSampleFromFile("/sample100.txt").filterNot(_.isEmpty)
    for( str <- samplesList ) {
      val deserializer = new RmsiMetricsDeserializer(null)
      val RmsiMetricsOption = deserializer.parseJson(str.getBytes)
      assert(deserializer.parseJson(str.getBytes).isDefined)
    }
  }


}

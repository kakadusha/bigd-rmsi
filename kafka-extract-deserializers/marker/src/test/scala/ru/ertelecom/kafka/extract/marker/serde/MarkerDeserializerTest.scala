package ru.ertelecom.kafka.extract.marker.serde

import org.scalatest.FunSuite
import ru.ertelecom.kafka.extract.marker.domain.MarkerParsed

class MarkerDeserializerTest extends FunSuite{

  test("message parses") {
    val deser = new MarkerDeserializer(null)
    val msg = "1\tv123456\t1605484799\tmarkter_test1"

    val actual = deser.parseTsv(msg.getBytes())

    val expected = MarkerParsed(
      triggered_date = "2020-11-15", triggered_ts = 1605484799, billing_id = 1, login = "v123456", marker = "markter_test1"
    )
    assert(expected == actual.get)
  }
}

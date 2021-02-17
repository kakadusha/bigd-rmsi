package ru.ertelecom.kafka.extract.recommendations.serde

import org.scalatest.FunSuite

class RecommendationsAnswerDeserializerTest extends FunSuite{

  test("successDeserializeAnswers") {
    val inputJson =
      """
        |{
        |    "id": "8b8ea8cc-8fdb-412c-92b7-219a522bc17d",
        |    "subscriberId": 13545990,
        |    "extIdCity": "chelny",
        |    "platformId": 44,
        |    "deviceId": 87498801,
        |    "segmentId": 1,
        |    "timeStamp": 1569833934,
        |    "showcaseItemType": "movie",
        |    "showcaseName": "uhd-films",
        |    "recommendationId": 100,
        |    "externalSortIds": [
        |        1787647,
        |        1307283
        |    ],
        |    "externalSortFields": [
        |        "nativeItemId",
        |        "nativeItemId"
        |    ],
        |    "recCount": 2
        | }
      """.stripMargin
    val payload = inputJson.getBytes
    val deserializer = new RecommendationsAnswersDeserializer(null)
    val answersArray = deserializer.parseJson(payload).get
    assert(2 == answersArray.length)
    assert("2019-09-30 08:58:54" == answersArray(0).timestam)
    assert("8b8ea8cc-8fdb-412c-92b7-219a522bc17d" == answersArray(0).uuid)
    assert(13545990 == answersArray(0).subscriberId)
    assert("chelny" == answersArray(0).extIdCity)
    assert(44 == answersArray(0).platformId)
    assert(87498801 == answersArray(0).deviceId)
    assert(Option.apply(1) == answersArray(0).segmentId)
    assert("movie" == answersArray(0).showcaseItemType)
    assert("uhd-films" == answersArray(0).showcaseName)
    assert(100 == answersArray(0).recommendationId)
    assert(Option.apply(2) == answersArray(0).recCount)
  }

  test("failedDeserializeAnswers") {
    val inputJson = """{}"""
    val payload = inputJson.getBytes
    val deserializer = new RecommendationsAnswersDeserializer(null)
    val answersArray = deserializer.parseJson(payload)
    assert(answersArray.isEmpty)
  }
}

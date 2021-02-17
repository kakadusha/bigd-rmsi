package ru.ertelecom.kafka.extract.clickstream.util

import org.scalatest.{BeforeAndAfter, FunSuite}

class DpiGroupInMemoryStorageTest extends FunSuite with BeforeAndAfter{
  var storage: DpiGroupInMemoryStorage = _

  before {
    storage = new DpiGroupInMemoryStorage()
  }

  test("test matching") {
    val dpiGroup = storage.getByMachineName("chelny2-soft")
    assert("chelny" == dpiGroup.get.groupName)
  }

  test("test not matched") {
    val dpiGroup = storage.getByMachineName("unknownCity")
    assert(dpiGroup.isEmpty)
  }
  test("wrong case") {
    val dpiGroup = storage.getByMachineName("Irkutsk-4")
    assert(dpiGroup.isDefined)
    assert("irkutsk" == dpiGroup.get.groupName)
  }
  test("test caching") {
    val city = "chelny2-soft"
    val dpiGroup = storage.getByMachineName(city)
    assert("chelny" == dpiGroup.get.groupName)
    val cachedGroup = storage.cache().getIfPresent(city)
    assert(dpiGroup.isDefined)
    assert(cachedGroup == dpiGroup.get)
  }
}

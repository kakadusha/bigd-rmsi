package ru.ertelecom.kafka.extract.clickstream.util

import java.util.concurrent.TimeUnit

import com.google.common.cache.{Cache, CacheBuilder}
import org.json4s.DefaultFormats

class DpiGroupInMemoryStorage {

  private val _groups: Array[DpiGroup] = Array(
    DpiGroup(cityId = 1, port = 20501, groupName = "perm", utcTimeDelta = 18000),
    DpiGroup(cityId = 2, port = 20500, groupName = "samara", utcTimeDelta = 14400),
    DpiGroup(cityId = 136, port = 20502, groupName = "nn", utcTimeDelta = 10800),
    DpiGroup(cityId = 136, port = 20502, groupName = "nnov", utcTimeDelta = 10800),
    DpiGroup(cityId = 215, port = 20503, groupName = "volgograd", utcTimeDelta = 10800),
    DpiGroup(cityId = 228, port = 20505, groupName = "tmn", utcTimeDelta = 18000),
    DpiGroup(cityId = 228, port = 20505, groupName = "tumen", utcTimeDelta = 18000),
    DpiGroup(cityId = 231, port = 20507, groupName = "chelny", utcTimeDelta = 10800),
    DpiGroup(cityId = 237, port = 20508, groupName = "kzn", utcTimeDelta = 10800),
    DpiGroup(cityId = 237, port = 20508, groupName = "kazan", utcTimeDelta = 10800),
    DpiGroup(cityId = 244, port = 20506, groupName = "omsk", utcTimeDelta = 21600),
    DpiGroup(cityId = 552, port = 20509, groupName = "saratov", utcTimeDelta = 10800),
    DpiGroup(cityId = 557, port = 20504, groupName = "ekat", utcTimeDelta = 18000),
    DpiGroup(cityId = 903, port = 20511, groupName = "irkutsk", utcTimeDelta = 28800),
    DpiGroup(cityId = 1100, port = 20510, groupName = "rostov", utcTimeDelta = 10800),
    DpiGroup(cityId = 554, port = 20512, groupName = "barnaul", utcTimeDelta = 25200),
    DpiGroup(cityId = 900, port = 20513, groupName = "bryansk", utcTimeDelta = 10800),
    DpiGroup(cityId = 901, port = 20514, groupName = "cheb", utcTimeDelta = 10800),
    DpiGroup(cityId = 241, port = 20515, groupName = "chel", utcTimeDelta = 18000),
    DpiGroup(cityId = 233, port = 20516, groupName = "izhevsk", utcTimeDelta = 14400),
    DpiGroup(cityId = 232, port = 20517, groupName = "kirov", utcTimeDelta = 10800),
    DpiGroup(cityId = 555, port = 20518, groupName = "krsk", utcTimeDelta = 25200),
    DpiGroup(cityId = 801, port = 20519, groupName = "kurgan", utcTimeDelta = 18000),
    DpiGroup(cityId = 907, port = 20520, groupName = "kursk", utcTimeDelta = 10800),
    DpiGroup(cityId = 551, port = 20521, groupName = "lipetsk", utcTimeDelta = 10800),
    DpiGroup(cityId = 802, port = 20522, groupName = "mgn", utcTimeDelta = 18000),
    DpiGroup(cityId = 802, port = 20522, groupName = "mng", utcTimeDelta = 18000),
    DpiGroup(cityId = 178, port = 20523, groupName = "nsk", utcTimeDelta = 25200),
    DpiGroup(cityId = 242, port = 20524, groupName = "oren", utcTimeDelta = 18000),
    DpiGroup(cityId = 238, port = 20525, groupName = "penza", utcTimeDelta = 10800),
    DpiGroup(cityId = 800, port = 20526, groupName = "ryazan", utcTimeDelta = 10800),
    DpiGroup(cityId = 556, port = 20527, groupName = "spb", utcTimeDelta = 10800),
    DpiGroup(cityId = 902, port = 20528, groupName = "tomsk", utcTimeDelta = 25200),
    DpiGroup(cityId = 803, port = 20529, groupName = "tula", utcTimeDelta = 10800),
    DpiGroup(cityId = 906, port = 20530, groupName = "tver", utcTimeDelta = 10800),
    DpiGroup(cityId = 804, port = 20531, groupName = "ufa", utcTimeDelta = 18000),
    DpiGroup(cityId = 805, port = 20532, groupName = "ulsk", utcTimeDelta = 14400),
    DpiGroup(cityId = 553, port = 20533, groupName = "voronezh", utcTimeDelta = 10800),
    DpiGroup(cityId = 558, port = 20534, groupName = "yar", utcTimeDelta = 10800),
    DpiGroup(cityId = 236, port = 20535, groupName = "yola", utcTimeDelta = 10800)
  )
  private val _cache: Cache[String, DpiGroup] = CacheBuilder
    .newBuilder()
    .expireAfterWrite(5, TimeUnit.HOURS)
    .build[String, DpiGroup]
  private val _cityPattern = "^([a-zA-Z]{2,})".r
  implicit val formats: DefaultFormats.type = DefaultFormats


  def getByMachineName(city: String): Option[DpiGroup] = {
    val cached = _cache.getIfPresent(city)
    if (cached == null) {
      val matched = _cityPattern.findFirstIn(city)
      if (matched.isEmpty) {
        return Option.empty
      }
      val matchedDpiGroups = _groups.find(group => group.groupName.toLowerCase == matched.get.toLowerCase)
      if (matchedDpiGroups.isEmpty) {
        return Option.empty
      }
      val dpiGroup = matchedDpiGroups.get
      _cache.put(city, dpiGroup)
      Option.apply(dpiGroup)
    } else {
      Option.apply(cached)
    }
  }

  def cache(): Cache[String, DpiGroup] = _cache

}

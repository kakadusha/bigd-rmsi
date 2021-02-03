package ru.ertelecom.kafka.extract.pixel_info.serde

import org.scalatest.FunSuite

class PixelInfoDeserializerTestSuite extends FunSuite{
    test("test_with_all_optional_filled") {
        val deserializer = new PixelInfoDeserializer(null)
        val input = "2020-04-02T08:18:10+00:00\t5.16.120.116\t/info.gif?u=http://info.ertelecom.ru/?campId=26476" +
          "&machine=spb&ourl=http%3A%2F%2Fconstructor-spb.academyey.com%2F&u=260D9BC8FE9EDB7D32BDD52D4FCDBDB1" +
          "&timestamp$c=1585815678&sid$c=4a2f7c3fcd896ae6e7f1837c320aae5f\tMozilla/5.0 (Windows NT 10.0; Win64; x64; " +
          "rv:74.0) Gecko/20100101 Firefox/74.0\tuid=uidequals0x1337\tp_uid=589CBABCD455385E45130DA702C381C1\thttp_ref=" +
          "http://info.ertelecom.ru/?campId=26476&machine=spb&ourl=http%3A%2F%2Fconstructor-spb.academyey.com%2F" +
          "&u=260D9BC8FE9EDB7D32BDD52D4FCDBDB1&timestamp$c=1585815678&sid$c=4a2f7c3fcd896ae6e7f1837c320aae5f\tspb"
        val pxi_record = deserializer.parseTsv(input)
        assert(pxi_record.get.datetime == 1585815490000L)
        assert(pxi_record.get.ip == "5.16.120.116")
        assert(pxi_record.get.pixel_endpoint == "/info.gif?u=http://info.ertelecom.ru/?campId=26476&machine=spb&ourl=http%3A%2F%2Fconstructor-spb.academyey.com%2F&u=260D9BC8FE9EDB7D32BDD52D4FCDBDB1&timestamp$c=1585815678&sid$c=4a2f7c3fcd896ae6e7f1837c320aae5f")
        assert(pxi_record.get.user_agent == "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0")
        assert(pxi_record.get.uid == Option.apply("uidequals0x1337"))
        assert(pxi_record.get.p_uid == "589CBABCD455385E45130DA702C381C1")
        assert(pxi_record.get.campid == Option.apply(26476) )
        assert(pxi_record.get.machine == Option.apply("spb"))
        assert(pxi_record.get.ourl == Option.apply("http://constructor-spb.academyey.com/"))
        assert(pxi_record.get.u == Option.apply("260D9BC8FE9EDB7D32BDD52D4FCDBDB1"))
        assert(pxi_record.get.timestamp_http_ref == Option.apply(1585815678000L) )
        assert(pxi_record.get.sid == Option.apply("4a2f7c3fcd896ae6e7f1837c320aae5f"))
        assert(pxi_record.get.city == Option.apply("spb"))
        assert(pxi_record.get.year == 2020)
        assert(pxi_record.get.month == 4)
    }

    test("test_with_no_optional_filled") {
        val deserializer = new PixelInfoDeserializer(null)
        val input = "2020-12-30T08:18:10+00:00\t4.8.15.16\t/info.gif?u=http://info.ertelecom.ru/?campId=26476" +
          "&machine=spb&ourl=http%3A%2F%2Fconstructor-spb.academyey.com%2F&u=260D9BC8FE9EDB7D32BDD52D4FCDBDB1" +
          "&timestamp$c=1585815678&sid$c=4a2f7c3fcd896ae6e7f1837c320aae5f\tMozilla/5.0 (Windows NT 10.0; Win64; x64; " +
          "rv:74.0) Gecko/20100101 Firefox/74.0\tuid=\tp_uid=589CBABCD455385E45130DA702C381C1\thttp_ref=" +
          "http://info.ertelecom.ru/"
        val pxi_record = deserializer.parseTsv(input)
        assert(pxi_record.get.datetime == 1609316290000L)
        assert(pxi_record.get.ip == "4.8.15.16")
        assert(pxi_record.get.pixel_endpoint == "/info.gif?u=http://info.ertelecom.ru/?campId=26476&machine=spb&ourl=http%3A%2F%2Fconstructor-spb.academyey.com%2F&u=260D9BC8FE9EDB7D32BDD52D4FCDBDB1&timestamp$c=1585815678&sid$c=4a2f7c3fcd896ae6e7f1837c320aae5f")
        assert(pxi_record.get.user_agent == "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0")
        assert(pxi_record.get.uid == Option.empty)
        assert(pxi_record.get.p_uid == "589CBABCD455385E45130DA702C381C1")
        assert(pxi_record.get.campid == Option.empty )
        assert(pxi_record.get.machine == Option.empty)
        assert(pxi_record.get.ourl == Option.empty)
        assert(pxi_record.get.u == Option.empty)
        assert(pxi_record.get.timestamp_http_ref == Option.empty )
        assert(pxi_record.get.sid == Option.empty)
        assert(pxi_record.get.city == Option.empty)
        assert(pxi_record.get.year == 2020)
        assert(pxi_record.get.month == 12)
    }

    test("test_with_broken_input") {
        val deserializer = new PixelInfoDeserializer(null)
        val input = "4.8.15.16\t/info.gif?u=http://info.ertelecom.ru/?campId=26476" +
          "&machine=spb&ourl=http%3A%2F%2Fconstructor-spb.academyey.com%2F&u=260D9BC8FE9EDB7D32BDD52D4FCDBDB1" +
          "&timestamp$c=1585815678&sid$c=4a2f7c3fcd896ae6e7f1837c320aae5f\tMozilla/5.0 (Windows NT 10.0; Win64; x64; " +
          "rv:74.0) Gecko/20100101 Firefox/74.0\tuid=\tp_uid=589CBABCD455385E45130DA702C381C1\thttp_ref=" +
          "http://info.ertelecom.ru/"
        val pxi_record = deserializer.parseTsv(input)
        assert(pxi_record == Option.empty)
    }
}

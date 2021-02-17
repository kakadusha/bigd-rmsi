package ru.ertelecom.kafka.extract.netflow.serde

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.{Date, StringTokenizer}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.netflow.domain.NetflowHttps
import ru.ertelecom.udf.funcs.ip.IpTransform

class NetflowHttpsDeserializer(appConf: Config) extends Deserializer(appConf) {

  lazy val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    parseTsv(payload)
  }

  def tokenize(string: String, delimiter: Char): Array[String] = {
    val temp = new Array[String]((string.length / 2) + 1)
    var wordCount = 0
    var i = 0
    var j = string.indexOf(delimiter)
    while ( {
      j >= 0
    }) {
      temp({
        wordCount += 1
        wordCount - 1
      }) = string.substring(i, j)
      i = j + 1
      j = string.indexOf(delimiter, i)
    }
    temp({
      wordCount += 1
      wordCount - 1
    }) = string.substring(i)
    val result = new Array[String](wordCount)
    System.arraycopy(temp, 0, result, 0, wordCount)
    result
  }

  def parseTsv(payload: Array[Byte]): Option[NetflowHttps] = {
    val tsv = tokenize(new String(payload, StandardCharsets.UTF_8), '\t')
    if (tsv.length < 6) {
      return Option.empty
    }
    val cidOption = if (tsv(3) == "-") Option.empty else Option.apply(tsv(3))
    val timestamp = tsv(1).toLong
    val partDay = new Date(timestamp * 1000L)
    Option.apply(
      NetflowHttps(
        bid = tsv(0).toShort,
        ts = timestamp,
        wip = IpTransform.ip4ToLong(tsv(2)),
        cid = cidOption,
        sni = tsv(4),
        cip = tsv(5),
        forDay = dateFormat.format(partDay)
      )
    )
  }
}

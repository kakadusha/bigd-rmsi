package ru.ertelecom.kafka.extract.gray_nat.serde

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.serde.Deserializer
import ru.ertelecom.kafka.extract.gray_nat.domain.Nat
import ru.ertelecom.udf.funcs.ip.IpTransform

class NatDeserializer(appConfig: Config) extends Deserializer(appConfig) {

  override def deserialize(): UserDefinedFunction = udf { payload: Array[Byte] =>
    this.parseTsv(payload)
  }

  def parseTsv(payload: Array[Byte]) : Option[Nat] = {
    val tsv = new String(payload, StandardCharsets.UTF_8).split("\t", -1)
    if(tsv.length < 7) {
      return Option.empty
    }
    val natLogin =  if(tsv(1) == "-") Option.empty else Option.apply(tsv(1))

    val grayIpSplitted = tsv(3).split("\\.")
    val grayIpLong = IpTransform.ip4ToLong(tsv(3))

    val whiteIpLong = IpTransform.ip4ToLong(tsv(4))

    Option.apply(Nat(
      billingId = tsv(0).toShort,
      login = natLogin,
      `type` = tsv(2).toShort,
      grayIp = grayIpLong,
      whiteIp = whiteIpLong,
      startPort = tsv(5).toInt,
      endPort = tsv(6).toInt
    ))
  }
}

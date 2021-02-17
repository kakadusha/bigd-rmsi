package ru.ertelecom.kafka.extract.netflow.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, ShortType}
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.transform.Transform
import ru.ertelecom.udf.funcs.ip.IpTransform

class NetflowTransform(appConf: Config) extends Transform(appConf){
  override def transform(dfIn: DataFrame): DataFrame = {
    val dfWithExploded = dfIn.select(
      split(col("nested"), "\t").as("tsvArr")
    )
    val ipTolongWrapper = udf{ ip: String =>
      IpTransform.ip4ToLong(ip)
    }
    dfWithExploded.select(
      col("tsvArr").getItem(0).cast(ShortType).as("bid"),
      col("tsvArr").getItem(1).cast(LongType).as("ts"),
      ipTolongWrapper(col("tsvArr").getItem(2)).as("wip"),
      when(not(col("tsvArr").getItem(3).equalTo("-")) , col("tsvArr").getItem(3)).as("cid"),
      col("tsvArr").getItem(4).as("sni"),
      col("tsvArr").getItem(5).as("cip"),
      to_date(from_unixtime(col("tsvArr").getItem(1).cast(LongType))).as("for_day")
    )
  }
}

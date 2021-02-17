package ru.ertelecom.kafka.extract.gray_nat.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.transform.Transform

class NatAddPartitioningColumnsTransform(appConfig: Config) extends Transform(appConfig){
  override def transform(df: DataFrame): DataFrame = {
    df.withColumn("year", year(col("timestamp").cast(TimestampType)))
      .withColumn("month", month(col("timestamp").cast(TimestampType)))
      .withColumn("day", dayofmonth(col("timestamp").cast(TimestampType)))
      .withWatermark("timestamp", "10 seconds")
      .dropDuplicates("billingId", "login", "type", "grayIp", "whiteIp", "startPort", "endPort")
  }
}

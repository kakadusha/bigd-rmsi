package ru.ertelecom.kafka.extract.sniffers.transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ru.ertelecom.kafka.extract.core.conf.Config
import ru.ertelecom.kafka.extract.core.transform.Transform

class AddMonthYearByTsCol(appConf: Config) extends Transform(appConf) {
  override def transform(dfIn: DataFrame): DataFrame = {
    dfIn
      .withColumn("year", year(from_unixtime(col("ts"))))
      .withColumn("month", month(from_unixtime(col("ts"))))
  }
}

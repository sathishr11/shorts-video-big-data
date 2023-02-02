import com.databricks.spark.xml._
import spark.implicits._

val df = spark.read.format("csv").option("header", "true").load(" /tmp/data_gen.csv")

val dftoWrite = df.withColumn("batchid",lit("2020033022"))

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
dftoWrite.write.insertInto("video_analytics.Merged_data_all")

val xmlData = spark.read.format("com.databricks.spark.xml").option("rowTag", "record").load("/tmp/data_gen.xml")
val xmlDataWrite = xmlData .withColumn("batchid",lit("2020013115"))
xmlDataWrite.write.insertInto("video_analytics.Merged_data_all")

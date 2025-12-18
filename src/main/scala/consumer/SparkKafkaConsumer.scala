package consumer

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import utils.BloomFilter

object SparkKafkaConsumer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkKafkaConsumer")
      .master("local[*]")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/")
      .config("spark.mongodb.output.database", "dashboard_news")
      .config("spark.mongodb.output.collection", "news")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")



    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark-news-stream-v2")
      .option("startingOffsets", "latest")
      .load()

    val parsedDF = kafkaDF
      .selectExpr("CAST(value AS STRING) AS json", "timestamp")
      .select(
        from_json(
          col("json"),
          StructType(Seq(
            StructField("title", StringType),
            StructField("link", StringType),
            StructField("content", StringType),
            StructField("date", StringType),
            StructField("source", StringType)
          ))
        ).alias("news"),
        col("timestamp")
      )
      .select(
        col("news.title"),
        col("news.link"),
        col("news.date"),
        col("news.content"),
        col("news.source"),
        col("timestamp")
      )


    val bloomFilteredParsedDF = parsedDF.filter { row =>

      val link  = row.getAs[String]("link")
      val title = row.getAs[String]("title")

      if (link == null || title == null) {
        println("[BLOOM] NULL item dropped")
        false
      } else {

        val bloomKey = s"$link||$title"

        if (!BloomFilter.mightContain(bloomKey)) {

          println("[BLOOM] NEW item accepted")
          BloomFilter.add(bloomKey)
          true

        } else {

          println("[BLOOM] DUPLICATE item ignored")
          false
        }
      }
    }

    val cleanedDF = bloomFilteredParsedDF
      .withColumn(
        "clean_title",
        trim(
          regexp_replace(
            col("title"),
            "[^\\u0600-\\u06FFa-zA-Z0-9\\s%:]",
            ""
          )
        )
      )
      .withColumn(
        "clean_content",
        trim(
          regexp_replace(
            col("content"),
            "[^\\u0600-\\u06FFa-zA-Z0-9\\s%:]",
            ""
          )
        )
      )
      .withColumn(
        "clean_title",
        regexp_replace(col("clean_title"), "\\s+", " ")
      )
      .withColumn(
        "clean_content",
        regexp_replace(col("clean_content"), "\\s+", " ")
      )


    val finalWithDateDF = cleanedDF
      .withColumn("parsed_date", to_timestamp(col("date")))


    val query = finalWithDateDF.writeStream
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("3 seconds"))
      .outputMode("append")
      .foreachBatch { (batchDF: Dataset[Row], _: Long) =>

        batchDF
          .select(
            col("clean_title").as("title"),
            col("clean_content").as("content"),
            col("source"),
            col("parsed_date").as("date"),
            col("link"),
            col("timestamp")
          )
          .write
          .format("com.mongodb.spark.sql.DefaultSource")
          .mode("append")
          .save()
      }
      .start()

    query.awaitTermination()
  }
}

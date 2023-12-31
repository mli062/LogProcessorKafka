package com.example.logprocessor

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, count, from_json, from_unixtime, when, window}
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructType, TimestampNTZType}


/**
 *
 */
object App {

  def main(args : Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Log Processor Kafka")
      .getOrCreate();
    println("Spark Version : " + spark.version);

    val kafkaConsumer = spark
      .readStream
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "connexion_logs")
      .option("startingOffsets", "earliest")
      .load()
    
    val connectionLogStrDF = kafkaConsumer.selectExpr("CAST(value AS STRING)")

    val connectionLogSchema = new StructType()
      .add(name = "timestamp", dataType = LongType)
      .add(name = "username", dataType = StringType)
      .add(name = "ip_address", dataType = StringType)
      .add(name = "country", dataType = StringType)
      .add(name = "port", dataType = IntegerType)
      .add(name = "connection_type", dataType = StringType)
      .add(name = "auth_success", dataType = BooleanType)
      .add(name = "password", dataType = StringType)

    val connectionLogDF = connectionLogStrDF
      .select(from_json(col("value"), connectionLogSchema).as("data"))
      .select("data.*")
      .withColumn("timestamp", from_unixtime(col("timestamp")))

    val aggregatedDF = connectionLogDF
      .groupBy(window(col("timestamp"), "10 minutes").as("Timestamp"))
      .agg(
        count("*").as("total_connections"),
        count(when(col("auth_success"), 1)).as("successful_connections"),
        count(when(!col("auth_success"), 1)).as("failed_connections")
      )
      .orderBy(asc("Timestamp"))

    val query = aggregatedDF
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}

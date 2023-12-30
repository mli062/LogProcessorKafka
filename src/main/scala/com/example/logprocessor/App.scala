package com.example.logprocessor

import org.apache.spark.sql.SparkSession

/**
 *
 */
object App {
    
  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Log Processor Kafka")
      .getOrCreate();
    println("Spark Version : " + spark.version);
  }

}

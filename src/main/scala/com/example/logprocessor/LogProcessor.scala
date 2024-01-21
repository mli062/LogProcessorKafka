package com.example.logprocessor

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{asc, col, count, from_unixtime, regexp_extract, unix_timestamp, when, window}
import org.apache.spark.sql.types.IntegerType

import scala.util.matching.Regex

object LogProcessor {

  // timestamp format
  val timestampLogFormat = "MMM dd HH:mm:ss"

  // Regular expressions to extract log information
  val timestampRegex: Regex = """(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})""".r
  val usernameRegex: Regex = """password for (invalid user\s+)?(\w+) from""".r
  val ipRegex: Regex = """from\s+([\d\.]+)\s+port""".r
  val portRegex: Regex = """port\s+(\d+)""".r

  /**
   * Parse connection logs and extract relevant information.
   *
   * @param connectionLogStrDF DataFrame containing log messages.
   * @return DataFrame with parsed log information.
   */
  def parseConnectionLog(connectionLogStrDF: Dataset[Row]): Dataset[Row] = {
    connectionLogStrDF.select(
      from_unixtime(unix_timestamp(regexp_extract(col("value"), timestampRegex.toString, 1), timestampLogFormat)).as("timestamp"),
      regexp_extract(col("value"), usernameRegex.toString(), 2).as("username"),
      regexp_extract(col("value"), ipRegex.toString(), 1).as("ip"),
      regexp_extract(col("value"), portRegex.toString(), 1).cast(IntegerType).as("port"),
      when(col("value").contains("Failed"), false).otherwise(true).as("connection_status"),
      when(col("value").contains("invalid user"), false).otherwise(true).as("user_existence")
    )
  }

  /**
   * Perform analytics on parsed connection logs.
   *
   * @param parsedConnectionLogDF DataFrame with parsed log information.
   * @return DataFrame with connection status counts.
   */
  def connectionStatusCount(parsedConnectionLogDF: Dataset[Row]): Dataset[Row] = {
    parsedConnectionLogDF
      .groupBy(window(col("timestamp"), "10 minutes").as("Timestamp"))
      .agg(
        count("*").as("total_connections"),
        count(when(col("connection_status"), 1)).as("successful_connections"),
        count(when(!col("connection_status"), 1)).as("failed_connections")
      )
      .orderBy(asc("Timestamp"))
  }

}

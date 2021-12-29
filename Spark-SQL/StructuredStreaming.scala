package in.iitpkd.scala

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/** Find the movies with the most ratings. */
object StructuredStreaming {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print error s
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName(name="StructuredStreaming")
      .master(master="local[*]")
      .getOrCreate()

    // Streaming source that monitors the data/logs directory for text files
    val accessLines = spark.readStream.text(path="data/logs")

    // Regular expressions to extract pieces of Apache access log lines
    val contentSizeExp = "\\s(\\d+)$"
    val statusExp = "\\s(\\d{3})\\s"
    val generalExp = "\"(\\S+)\\s(\\S+)\\s*(\\S*)\""
    val timeExp = "\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}-\\d{4})]"
    val hostExp = "(^\\S+\\.[\\S+\\.]+\\S+)\\s"

    // Apply these regular expressions to create structure from the unstructured text
    val logsDF = accessLines.select(regexp_extract(col(colName = "value"), hostExp, groupIdx=1).alias(alias="host"),
      regexp_extract(col(colName="value"), timeExp, groupIdx = 1).alias(alias="timestamp"),
      regexp_extract(col(colName="value"), generalExp, groupIdx = 1).alias(alias="method"),
      regexp_extract(col(colName="value"), generalExp, groupIdx = 2).alias(alias="endpoint"),
      regexp_extract(col(colName="value"), generalExp, groupIdx = 3).alias(alias="protocol"),
      regexp_extract(col(colName="value"), statusExp, groupIdx =  1).cast(to="Integer").alias(alias="status"),
      regexp_extract(col(colName="value"), contentSizeExp, groupIdx = 1).cast(to = "Integer").alias(alias="content_size"))

    // Keep a running count of status codes
    val statusCountsDF = logsDF.groupBy(col1="status").count()

    // Display the stream to the console
    val query = statusCountsDF.writeStream.outputMode(outputMode = "complete").format(source="console").queryName(queryName="counts").start()

    // Wait until we terminate the scripts
    query.awaitTermination()

    // Stop the session
    spark.stop()
  }

}

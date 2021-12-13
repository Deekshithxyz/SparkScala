package in.iitpkd.scala

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext._


object A {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName(name = "StructuredStreaming")
      .master(master = "local[*]")
      .getOrCreate()

    val accessLines = spark.readStream.text(path = "data/logs")

    import spark.implicits._

    // Regular expressions to extract pieces of Apache access log lines
    val userExp = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"(.+?)\""



    // Apply these regular expressions to create structure from the unstructured text
    val logsDF = accessLines.select(regexp_extract(col(colName = "value"), userExp, groupIdx = 4).alias(alias = "user_agent"),
      regexp_extract(col(colName = "value"), userExp, groupIdx = 8).alias(alias = "ip_address"))

    val f = logsDF.withColumn("user",translate($"user_agent","/","-")).drop("user_agent")

    val g = f.withColumn("use_a",translate($"user","Nov","11")).drop("user")

    val h = g.withColumn("datetime",translate($"use_a","Dec","12")).drop("use_a")

    val i = h.withColumn("datetim",translate($"datetime","+0000","000")).drop("datetime")

    val j = i.withColumn("datetime_timestamp",to_timestamp(col("datetim"), "dd-MM-yyyy:HH:mm:ss SSSSS"))

    val k = j.withColumn("window",window($"datetime_timestamp","30 Seconds","10 Seconds")).drop("datetim")

    val l = k.groupBy($"window", $"ip_address").count().orderBy(asc("window"), desc("count"))

    val query = l.writeStream.outputMode(outputMode="complete").format(source = "console").start()

    // Wait until we terminate the scripts
    query.awaitTermination()

    // Stop the session
    spark.stop()
  }

}


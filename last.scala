// Run this code after running "ncat -kl 9999 < access_log.txt" on the command prompt first. Otherwise the output is not observed

package in.iitpkd.scala

import org.apache.log4j._
import org.apache.spark.sql._

import org.apache.spark.sql.functions._


object last {
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName(name = "SparkStreamingSQL")
      .master(master = "local[*]")
      .getOrCreate()


    val accessLines = spark.readStream.format("socket")
      .option("host", "localhost").option("port", 9999).load()

    // Regular expressions to extract pieces of Apache access log lines
    val userExp = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"(.+?)\""

    //  Apply these regular expressions to create structure from the unstructured text
    val logsDF = accessLines.select(regexp_extract(col(colName = "value"), userExp, groupIdx = 9).alias(alias = "user_agent"))

    logsDF.createOrReplaceTempView(viewName = "logDF")

    // Keep a running count of status codes
    val statusCountsDF = spark.sql(sqlText="SELECT user_agent, COUNT(*) FROM logDF GROUP BY user_agent")

    // Display the stream to the console
    val query = statusCountsDF.writeStream.outputMode("complete").format(source = "console").start()

    // Wait until we terminate the scripts
    query.awaitTermination()
    // Stop the session
    spark.stop()
  }
}

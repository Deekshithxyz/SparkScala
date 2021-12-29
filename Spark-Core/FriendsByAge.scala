package in.iitpkd.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._


object FriendsByAge {
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String): (Int, Int) = {
    // Split by commas
    val fields = line.split(",")
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    // Create a tuple that is our result
    (age, numFriends)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext(master="local[*]",appName="FriendsByAge")

    // Load each line of the source data into an RDD
    val lines = sc.textFile(path="data/friends.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the ja)
    val results = averagesByAge.collect()

    // Sort and print the final results.
    results.sorted.foreach(println)
  }
}

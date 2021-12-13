package in.iitpkd.scala

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Count up how many of each word occurs in a book, using regular expressions and sorting */
object WordCountDataSet {

  case class Book(value: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName(name="WordCount")
      .master(master="local[*]")
      .getOrCreate()

    // Read each line of my book into an Dataset
    import spark.implicits._
    val input = spark.read.text(path="data/The_Hunger_Games.txt").as[Book]

    // Split using a regular expression that extracts words
    val words = input
       .select(explode(split( str= $"value", pattern="\\W+")).alias(alias="word"))
       .filter(condition=$"word" =!= "")

    // Normalize everything to lowercase
    val lowercaseWords = words.select(lower($"word")).alias(alias="word")

    // Count up the occurrences of each word
    val wordCounts = lowercaseWords.groupBy(col1="word").count()

    // Sort by counts
    val wordCountsSorted = wordCounts.sort(sortCol = "count")

    // Show the results
    wordCountsSorted.show(wordCountsSorted.count.toInt)

    // ANOTHER WAY TO DO IT (Blending RDD's and Datasets)
    val bookRDD = spark.sparkContext.textFile(path="data/The_Hunger_Games.txt")
    val wordsRDD = bookRDD.flatMap(x => x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()

    val lowercaseWordsDS = wordsDS.select(lower($"value").alias(alias="word"))
    val wordCountsDS = lowercaseWordsDS.groupBy(col1="word").count()
    val wordCountsSortedDS = wordCountsDS.sort(sortCol = "count")
    wordCountsSortedDS.show(wordCountsSortedDS.count.toInt)

  }
}
  
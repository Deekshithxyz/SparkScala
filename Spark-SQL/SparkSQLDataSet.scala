package in.iitpkd.scala
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQLDataSet {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
     Logger.getLogger("org").setLevel(Level.ERROR)

    // Use SparkSession interface
    val spark = SparkSession
      .builder
      .appName(name="SparkSQL")
      .master(master="local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val schemaPeople = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv( path = "data/friends.csv")
      .as[Person]

    schemaPeople.printSchema()

    schemaPeople.createOrReplaceTempView(viewName = "people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    val results = teenagers.collect()

    results.foreach(println)

    spark.stop()
  }
}


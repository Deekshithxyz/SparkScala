package in.iitpkd.scala
import org.apache.spark.sql._
import org.apache.log4j._

object b {

  case class Record(stationid:String, year:Int, typ:String, temperature:Float,emp1:String,emp2:String,emp3:String)

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
    val Records = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv( path = "data/weather.csv")
      .as[Record]

    val prcptemp = Records.filter(Records("typ") === "PRCP")

    val extemp = prcptemp.withColumn("realtemp",prcptemp("temperature")/10)

    extemp.groupBy("stationid").avg("realtemp").show()

    spark.stop()
  }
}

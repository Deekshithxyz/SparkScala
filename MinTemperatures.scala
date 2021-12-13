package in.iitpkd.scala
import org.apache.spark._
import org.apache.log4j._
import scala.math.min

object MinTemperatures {
  def parseLine(line:String): (String, String, Float)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f
    (stationID, entryType, temperature)
  }
  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print the errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext(master = "local[*]", appName = "MinTemperatures")

    // Read each line of input data
    val lines = sc.textFile(path="data/weather.csv")

    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)

    // Filter out all but TMIN entries
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")

    // Convert to (stationID, temperature)
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))

    // Reduce by stationID retaining the minimum temperature found
    val minTempsByStation = stationTemps.reduceByKey((x,y) => min(x,y))

    // Collect, format, and print the results
    val results = minTempsByStation.collect()

    for (result <- results.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f C"
    }
  }
}
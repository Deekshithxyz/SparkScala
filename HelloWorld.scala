package in.iitpkd.scala

import org.apache.spark._
import org.apache.log4j._

object HelloWorld {
  def main(args: Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext(master="local[*]", appName = "HelloWorld")

    val lines = sc.textFile(path="data/ml-100k/u.data")
    val numLines = lines.count()

    println("Hello world! The u.data file has " + numLines + " lines.")

    sc.stop()

  }
}
 
package in.iitpkd.scala

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans

object C {
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(master="local[*]", appName="C", Seconds(1))

    setupLogging()

    val accessLines = ssc.socketTextStream(hostname = "127.0.0.1", port=9999, StorageLevel.MEMORY_AND_DISK_SER)

    

    // Kick it off
    ssc.checkpoint(directory="C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()

  }

}

package in.iitpkd.scala

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans
/** Example of using streaming K-Means clustering to cluster people by income and age in
 * 5 clusters.
 */
object StreamingKMeans {
  def setupLogging(): Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }
  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(master="local[*]", appName="StreamingKMeans", Seconds(1))

    setupLogging()
    // Create a socket stream to listen for training data on port 9999
    // This will listen for [income, age] data (or anything else) that we want to cluster
    val trainingLines = ssc.socketTextStream(hostname = "127.0.0.1", port=9999, StorageLevel.MEMORY_AND_DISK_SER)

    val testingLines = ssc.socketTextStream(hostname="127.0.0.1", port=7777, StorageLevel.MEMORY_AND_DISK_SER)
    // Convert input data to Vectors and LabeledPoints for the MLLib functions we will
    val trainingData = trainingLines.map(Vectors.parse).cache()
    val testData = testingLines.map(LabeledPoint.parse)
    // just so we see something happen when training data is received
    trainingData.print()

    // Build a K-Means clustering model for 5 clusters and 2 features (age and income)
    val model = new StreamingKMeans()
      .setK(5)
      .setDecayFactor(1.0)
      .setRandomCenters(dim=2, weight=0.0)

    model.trainOn(trainingData)

    // And as test data is received, we'll keep refining our clustering model and printing out the
    // results. In the real world, we'd just use predictOn() which only expects feature data, as you
    // wouldn't know the "correct" clustering ahead of time. But in this case we print the cluster ID's
    // we assigned in the test data alongside the predicted cluster ID's. The ID's themselves don't have
    // to match, but the clustering should be more or less consistent.

    model.predictOnValues(testData.map(lp => (lp.label.toInt, lp.features))).print()

    // Kick it off
    ssc.checkpoint(directory="C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()

  }

}

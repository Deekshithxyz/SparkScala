package in.iitpkd.scala

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressor

object DecisionTree {

  case class RegressionSchema(No:Double,TransactionDate:Double,HouseAge:Double,DistanceToMRT:Double,NumberConvenienceStores:Double,Latitude:Double,Longitude:Double,PriceOfUnitArea:Double)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("DecisionTreeDF")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val dsRaw = spark.read
      .option("header", "true")
      .option("inferSchema","true")
      .csv("data/realestate.csv")
      .as[RegressionSchema]

    val assembler = new VectorAssembler()
      .setInputCols(Array("TransactionDate","HouseAge","DistanceToMRT","NumberConvenienceStores","Latitude","Longitude"))
      .setOutputCol("features")

    val df1 = assembler.transform(dsRaw)
      .select(col="PriceOfUnitArea", cols="features")

    val df2 = df1.withColumn("label",df1("PriceOfUnitArea"))

    val df  = df2.select(col="label",cols="features")

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(df)

    // Let's split our data into training data and testing data
    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    // Now create our Decision Tree model
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, dt))

    val model = pipeline.fit(trainingDF)

    val fullPredictions = model.transform(testDF)

    val predictionAndLabel = fullPredictions.select(col="prediction", cols="label").collect()

    for (prediction <- predictionAndLabel) {
      println(prediction)
    }

    // Stop the session
    spark.stop()
  }
}

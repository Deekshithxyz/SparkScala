package in.iitpkd.scala

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.log4j._

object MostPopularSuperHeroDataSet{

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Hyderabad")
      .getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add(name = "id", IntegerType, nullable = true)
      .add(name = "name", StringType, nullable = true)

    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep"," ")
      .csv(path="data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text(path="data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn(colName="id", split(col(colName = "value"), pattern = " ")(0))
      .withColumn(colName="connections", size(split(col(colName="value"), pattern = " "))-1)
      .groupBy(col1="id").agg(sum(columnName="connections").alias(alias="connections"))

    val mostPopular = connections
      .sort($"connections".desc)
      .first()

    val mostPopularName = names
      .filter(condition=$"id"===mostPopular(0))
      .select(col="name")
      .first()

    println(s"${mostPopularName(0)} is the most popular superhero with ${mostPopular(1)} co-appearances.")
    spark.stop()
  }

}

package in.iitpkd.scala

import org.apache.spark._
import org.apache.log4j._

object Hyderabad{

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext(master="local[*]","RDD")

    val input = sc.textFile("data/The_Hunger_Games.txt")

    val words = input.flatMap(x=>x.split("\\W+"))

    val worda = words.map(x=>(x,1)).reduceByKey((x,y)=>x+y)

    for (x <- worda){
      println(x)
    }
  }
}
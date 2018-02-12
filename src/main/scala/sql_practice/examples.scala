package sql_practice

import org.apache.spark.sql.functions.{avg, explode, max, min}
import spark_helpers.SparkSessionHelper

import scala.collection.mutable


object examples {
  def exec1(): Unit ={
    val spark = SparkSessionHelper.getSparkSession()
    import spark.implicits._
    val tourDF = spark.read.option("multiline", true).option("mode", "PERMISSIVE").json("data/input/tours.json")

    println(tourDF.count())
    //tourDF.show()
    tourDF.printSchema()

    val a = tourDF.groupBy("tourDifficulty").count()
    //a.show()
    println("unique levels of difficulty : "+a.count())

    tourDF.agg(min("tourPrice"), max("tourPrice"), avg("tourPrice")).show()
    tourDF.groupBy("tourDifficulty").agg(min("tourPrice"), max("tourPrice"), avg("tourPrice")).show()

    tourDF.groupBy("tourDifficulty").agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"), min("tourLength"), max("tourLength"), avg("tourLength")).show()


    val b = tourDF.select(explode($"tourTags")).groupBy("col").count().sort($"count".desc)
    b.show(10)
    val c = tourDF.select(explode($"tourTags"), $"tourDifficulty").groupBy("col", "tourDifficulty").count().sort($"count".desc)
    c.show(10)
    val d = tourDF.select(explode($"tourTags"), $"tourDifficulty", $"tourPrice").groupBy("col", "tourDifficulty").agg(min("tourPrice"), max("tourPrice"), avg("tourPrice")).sort($"avg(tourPrice)".desc)
    d.show(10)
  }
}

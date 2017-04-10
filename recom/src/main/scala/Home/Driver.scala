package Home

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import play.api.libs.json._


object Driver {

  val weights = Map(
    "att-a" -> 26,
    "att-b" -> 25,
    "att-c" -> 24,
    "att-d" -> 23,
    "att-e" -> 22,
    "att-f" -> 21,
    "att-g" -> 20,
    "att-h" -> 19,
    "att-i" -> 18,
    "att-j" -> 17)

  def evaluateScore(articleObject: Map[String, String], querySKU: Map[String, String], weights: Map[String, Int]): (Int, Int) = {

    val resultantMap = weights.filter {
      case (key, value) =>
        (articleObject(key) == querySKU(key))
    }
    (resultantMap.size, resultantMap.values.sum)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Recommendations")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val filePath = args(0)
    val querySKU = args(1)
    
    val rawData = sc.textFile(filePath)
    val articlesRDD = sc.parallelize(rawData.first.replace("},", "}}\n{").split("\n"))

    val skuAttRDD = articlesRDD.map { x =>
      val articleObj = Json.parse(x).validate[Map[String, Map[String, String]]].get
      val attributesMap = articleObj.values.head
      val sku = articleObj.keys.head
      (sku, attributesMap)
    }

    val weightsBC = sc.broadcast(weights)
    val inputBC = sc.broadcast(skuAttRDD.lookup(querySKU).head)

    val scoredData = skuAttRDD.map {
      case (x, y) =>
        (x, y, evaluateScore(y, inputBC.value, weightsBC.value))
    }

    implicit def ordering[A <: (String, scala.collection.immutable.Map[String, String], (Int, Int))]: Ordering[A] = new Ordering[A] {
      override def compare(x: A, y: A): Int = {
        val v = x._3._1.compareTo(y._3._1)
        if (v != 0) {
          v
        } else {
          x._3._2.compareTo(y._3._2)
        }
      }
    }

    val topSimilar = scoredData.top(10)
    topSimilar.foreach(println)

  }
}
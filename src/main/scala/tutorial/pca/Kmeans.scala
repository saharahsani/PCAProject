package tutorial.pca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors

import java.time.{Duration, Instant}

object Kmeans {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[*]").setAppName("K-meansTest")
      .set("spark.ui.enabled", "True")
      .set("spark.ui.port", "4040")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // Load and parse the data
    val data = sc.textFile("src/resources/splitKdd2000.csv")
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble)))
    val start=Instant.now()

    val numClusters = 50
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    val end=Instant.now()
    val durationStep = Duration.between(start, end).toMillis
    println("elapsed time: "+durationStep + " ms")

  }
}

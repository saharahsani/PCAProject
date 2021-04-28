package tutorial.pca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

import java.io.FileWriter
import java.time.Instant

object KmeansWithPCA {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[*]").setAppName("PCATest")
      .set("spark.ui.enabled", "True")
      .set("spark.ui.port", "4040")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // Load 2000 record and parse the data
    val data = sc.textFile("src/resources/powersupply.csv")
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble)))
    val start = Instant.now()

    /**
     * PCA Phase
     */
    val mat: RowMatrix = new RowMatrix(parsedData)
    val pc: Matrix = mat.computePrincipalComponents(1)
    val projected: RowMatrix = mat.multiply(pc)

    val fileWritter = new FileWriter("src/resources/powerPCAC.csv", true);
    projected.rows.collect().foreach { x =>
      fileWritter.write(x.toArray.mkString(",") + "\n")

      /**
       * Kmeans Algorithm
       */
      /*val numClusters = 50
    val numIterations = 20
    val clusters = KMeans.train(projected.rows, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(projected.rows)
    println(s"Within Set Sum of Squared Errors = $WSSSE")
    val end = Instant.now()

    val durationStep = Duration.between(start, end).toMillis
    println("elapsed time: " + durationStep + " ms")
  */
    }
  }
}

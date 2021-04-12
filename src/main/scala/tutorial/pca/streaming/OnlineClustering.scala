package tutorial.pca.streaming

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

class OnlineClustering {
  private var N: Long = 0L
  private var time: Int = 0

  /**
   * for compute PCA
   * @param rdd
   * @return rdd of vector
   */
  def preProcessing(rdd: RDD[linalg.Vector]): RDD[org.apache.spark.mllib.linalg.Vector] = {
    val mat: RowMatrix = new RowMatrix(rdd)
    val pc: Matrix = mat.computePrincipalComponents(5)
    val projected: RowMatrix = mat.multiply(pc)
    projected.rows
  }

  /**
   * for clustering data
   * @param data
   */
  def clustering(data: RDD[linalg.Vector]) = {
    val numClusters = 50
    val numIterations = 20
    val clusters = KMeans.train(data, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(data)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

  }

  def Run(data: DStream[org.apache.spark.mllib.linalg.Vector]): Unit = {
    data.foreachRDD { rdd =>
      val currentN = rdd.count()
      if (currentN != 0) {

        // preProcessing Phase
        val preProcessedData = preProcessing(rdd)
        // clustering Phase
        clustering(preProcessedData)

        this.N += currentN
        this.time += 1
        println("time is: "+this.time)

      }
    }
  }
}

package tutorial.pca.streaming

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

import java.io.FileWriter

class OnlineClustering {

  private var N: Long = 0L
  private var time: Int = 0

  /**
   * for compute PCA
   *
   * @param rdd
   * @return rdd of vector
   */
  def preProcessing(rdd: RDD[linalg.Vector]): RDD[org.apache.spark.mllib.linalg.Vector] = {
    val mat: RowMatrix = new RowMatrix(rdd)
    val pc: Matrix = mat.computePrincipalComponents(20)
    val projected: RowMatrix = mat.multiply(pc)
    val spark = SparkSession.builder.getOrCreate()
    // projected.rows.map(x=>x.toArray.mkString(",")).repartition(1).toDF("asv").write.mode("append").csv("src/resources/powerPca")
    val fileWritter = new FileWriter("src/resources/testCovPCA.csv", true)
    projected.rows.collect().foreach { x =>
      fileWritter.write(x.toArray.mkString(",")+"\n")
    }
    fileWritter.close()
    projected.rows
  }

  /**
   * for clustering data
   *
   * @param data
   */
  def clustering(data: RDD[linalg.Vector]) = {
    val numClusters = 50
    val numIterations = 20
   // val clusters = KMeans.train(data, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
   // val WSSSE = clusters.computeCost(data)
  //  println(s"Within Set Sum of Squared Errors = $WSSSE")

  }

  def Run(data: DStream[org.apache.spark.mllib.linalg.Vector]): Unit = {
    data.foreachRDD { rdd =>
      val currentN = rdd.count()
      if (currentN != 0) {
        val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(rdd.map(x => x))
        // data2 will be unit variance and zero mean.
        val data_std = rdd.map(x => scaler2.transform(Vectors.dense(x.toArray)))
        // preProcessing Phase
        val preProcessedData = preProcessing(data_std)

        // clustering Phase
        clustering(preProcessedData)

        this.N += currentN
        this.time += 1
        println("time is: " + this.time + " N: "+ this.N)

      }
    }
  }
}

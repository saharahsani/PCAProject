package tutorial.pca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

import java.io.FileWriter

object SVDexample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[*]").setAppName("PCATest")
      .set("spark.ui.enabled", "True")
      .set("spark.ui.port", "4040")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    // Load 2000 record and parse the data
    val data = sc.textFile("src/resources/testKddLabel.csv").map(s => s.split('-'))
    val parsedData= data.map(x=>x(0).split(",").map(_.toDouble)).map(x=> Vectors.dense(x))

    val scaler2 = new StandardScaler(withMean = true, withStd = true).fit(parsedData.map(x => x))

    // data2 will be unit variance and zero mean.
    val data_std = parsedData.map(x => scaler2.transform(Vectors.dense(x.toArray)))
    val fileWritter = new FileWriter("src/resources/scaledTestKddLabel.csv" )
    data_std.collect().foreach { x =>
      fileWritter.write(x.toArray.mkString(",") + "\n")
    }
    fileWritter.close()
/*
    /**
     * SVD Phase
     */
    val mat: RowMatrix = new RowMatrix(data_std)
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(54, computeU = true)
    val U: RowMatrix = svd.U // The U factor is a RowMatrix.
    val S = svd.s // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V // The V factor is a local dense matrix.
    // U.rows.collect().foreach(x=>println(x))

    val sum_eigValue = S.toArray.sum
    println("s is: " + S)

    val fileWritter = new FileWriter("src/resources/eigValueCovtype1.csv" )
    S.toArray.sortBy(x=>x)(Ordering[Double].reverse).foreach { x =>
      val exp = (x / sum_eigValue) * 100
      fileWritter.write(exp.toString + "\n")
      println(exp)
    }
    fileWritter.close()*/
  }

}

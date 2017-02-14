/**
  * Created by Anthony on 09/02/2017.
  */

import breeze.numerics.floor
import com.quantifind.charts.Highcharts.histogram
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object variables_aleatoires extends Utils {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("variables-aleatoires")

    val sc = new SparkContext(conf)


    // Loi uniforme
//    histogram(loiUniforme(sc, 1, 7), 20)

    // Loi de Poisson
    // Rq : Loi de poisson converge vers la gaussienne à partir de 5 de fréquence (mean)
    histogram(loiPoisson(sc, 0.001), 20)

    // Loi Normale
    // Si on augmente l'incertitude, notre histogramme s'applatit
    // Si l'incertitude est trop grande, on converge vers une loi uniforme ==> manque d'information (je ne sais rien)

//    val union = loiNormale(sc, 180, 2).union(loiNormale(sc, 160, 3)).collect.toSeq
//    histogram(union, 30)

  }

}

trait Utils {

  def loiUniforme(sc : SparkContext, from: Int, to: Int, size: Int = 10000) = {
    RandomRDDs.uniformRDD(sc, size)
      .map(x => from + (to - from) * x)
      .collect
      .toSeq
  }

  def loiPoisson(sc: SparkContext, mean: Double, size: Int = 100000): Seq[Double] = {
    RandomRDDs.poissonRDD(sc, mean, size)
      .collect
      .toSeq
  }

  def loiNormale(sc: SparkContext, center: Double ,incertitudeFactor: Double = 1, size: Int = 100000): RDD[Double] = {
    RandomRDDs.normalRDD(sc, size, 1)
      .map(incertitude => center + incertitudeFactor * incertitude)
  }

}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover, Tokenizer}
import org.apache.spark.sql.SQLContext

/**
  * Created by Anthony on 12/01/17.
  */


object removerObject extends StopWordsRemover{
}

object stop_word_removal {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Naive Bernouilli Bayes")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val dataSetTest = sqlContext.createDataFrame(Seq(
      (0, Seq("I", "love", "data", "science")),
      (1, Seq("This", "is", "a", "beautiful", "house"))
    )).toDF("id", "raw")

    removerObject.setInputCol("raw").setOutputCol("filtered").transform(dataSetTest).show()

    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")


    val dataSet = sqlContext.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "baloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show()

    dataSet.show()
  }
}
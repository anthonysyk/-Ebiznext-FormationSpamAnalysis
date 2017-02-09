import breeze.numerics.log
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Anthony on 12/01/17.
  *
  *
  * TF - IDF = TF * IDF
  *
  *
  *
  *        nombre de mots w dans d
  * TF = ----------------------------
  *      nombre total de mots dans d
  *
  *
  *
  *
  *                nombre de documents du corpus c
  * IDF =  Log  ----------------------------------------
  *                nombre de documents de c contenant w
  *
  * ====> indique comment se comporte le mot dans mon corpus
  *
  *       Si un mot apparait dans tous les documents, il aura une faible importance (comme un stopword) et aura donc un
  *       TF-IDF faible
  *
  *       Ainsi, un mot qui apparait souvent, mais qui est discriminant (décisif) aura un poid faible
  *
  *       Pour la classification de mails, IDF n'est pas pertinent
  *
  * Rq: Le log permet d'adoucir IDF parceque le premier terme peut être bien plus grand que le second terme
  *
  */
object tf_idf {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("Naive Bernoulli Bayes")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {

    val corpus: RDD[String] = sc.parallelize(List(
      "Hello the, very big World, Hello",
      "Hello the Monde",
      "Hello dearly Kitty Kitta"))
      .map(document => document.replaceAll("""[\p{Punct}&&[^.]]""", "")) // split("\\w+")

    val N: Long = corpus.count

    val format: (Long) => String = (id: Long) => "doc_" + id

    val wordsprop: RDD[((String, String), Double)] = corpus
      .zipWithIndex()
      .flatMap {
        case (document: String, docid: Long) => {
          val docsize = document.split(" ").size
          document.split(" ").map(word => ((word, format(docid)), 1.0 / docsize)) }
      } // ((the,doc_1),1/3) ((the,doc_0),1/6) ((very,doc_0),1/6)

    val termFrequency: RDD[((String, String), Double)] = wordsprop.reduceByKey(_ + _)

    val documentFrequency: RDD[(String, Double)] = termFrequency.map {
      case ((word:String, docid: String), tf: Double) => (word, docid :: Nil)
    }.reduceByKey(_ ++ _)
      .map{ case (word:String, doclist: List[String]) => (word, log(doclist.length))}

    import sqlContext.implicits._
    val wpDF = wordsprop.toDF("word-document", "word-proportion").show
    val tfDF = termFrequency.toDF("word-document", "term-frequency").show
    val dfDF = documentFrequency.toDF("word", "document-frequency").show

//    val newNames = Seq("id", "x1", "x2", "x3")
//    val dfRenamed = df.toDF(newNames: _*)


  }


}

import org._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{NGram, Tokenizer}
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.tartarus._
import org.tartarus.snowball._

import scala.tools.nsc.transform.Transform



/* SimpleApp.scala */

case class Data(value: String, id: Int, stemmed: String)

case class Word(id: Long, word: String, stemmed: String)

object SimpleApp {
  def main(args: Array[String]) {
//    val logFile = "file:///Users/haridas/Projects/DataScience/RPX/LDA/insight_engine-master/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    conf.setMaster("spark://172.17.2.125:7077")
    val sc = new SparkContext(conf)
//    //val logData = sc.textFile('working', 1).cache()
//    //val logData = sc.textFile('working', 1).cache()
//    val logData = "haridas N \n working on different things."
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))


//    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sparkSession = SparkSession.builder().getOrCreate();
    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._
    // For implicit conversions like converting RDDs to DataFrames
    // this is used to implicitly convert an RDD to a DataFrame.


//    val data = sparkSession.createDataFrame(Seq( Seq("testing", 1, ""), Seq("test", 2, ""), Seq("tester", 3, "")).toDF()


    //val data = Seq("test", "testing", "tester").toDS()

    val dataset = Seq(Word(0, "hello", ""), Word(1, "spark", "")).toDS

    val datasetDf = sparkSession.createDataFrame(Seq(
      (1L, "haridas close, closing closer closed the document... testing"),
      (2L, "Module was tested by Amos"),
      (3L, "Tested by Haridas")
    )).toDF("id", "word")


    val tokenizer = new Tokenizer()
      .setInputCol("word")
      .setOutputCol("tokens")

    val stemmer = new Stemmer()
      .setInputCol("tokens")
      .setOutputCol("stemmed")
      .setLanguage("english")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stemmer))
    val model = pipeline.fit(datasetDf)

    model.transform(datasetDf)
      .select("id", "tokens", "stemmed")
      .collect()
      .foreach{ case Row(id: Long, tokens: Array[String], stemmed: Array[String]) =>
          println(s"ID: $id , tokens: $tokens , stemmed: $stemmed")
      }



//    model.write.overwrite().save("/tmp/stemmer.log")


//    dataset.show()
  }
}
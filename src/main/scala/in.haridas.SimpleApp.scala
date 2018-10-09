import org._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql.SparkSession
import org.tartarus._
import org.tartarus.snowball._

/* SimpleApp.scala */

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
    val df = SparkSession.builder().getOrCreate();
    // this is used to implicitly convert an RDD to a DataFrame.

    val data = df.createDataFrame(
      Seq(("testing", 1, ""), ("test", 2, ""), ("tester", 3, ""))
    ).toDF("word", "id", "stemmed")

    val stemmer = new Stemmer()
      .setInputCol("word")
      .setOutputCol("stemmed")
      .setLanguage("english")
      .transform(data)

    stemmer.show()

    data.show()
  }
}
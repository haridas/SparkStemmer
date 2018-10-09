name := "SparkStemmer"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.2" % "runtime"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"

libraryDependencies += "com.github.master" %% "spark-stemming" % "0.2.2"
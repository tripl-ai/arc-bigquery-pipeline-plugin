import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "2.4.5"
  lazy val hadoopVersion = "2.9.2"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "2.11.0" % "provided"

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  val spark_big_query = "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.15.1-beta"

  // Project
  val etlDeps = Seq(
    scalaTest,
    
    arc,

    sparkSql,

    spark_big_query
  )
}
import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "3.0.1"
  lazy val hadoopVersion = "3.2.0"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "3.7.0" % "provided"

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  val sparkBigQuery = "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.19.1"

  // Project
  val etlDeps = Seq(
    scalaTest,
    arc,
    sparkSql,
    sparkBigQuery
  )
}
package ai.tripl.arc.execute

import java.net.URI
import java.util.UUID

import scala.collection.JavaConverters._

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.ControlUtils._
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.JDBCUtils
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQueryException
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Job
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.JobInfo
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.JobId
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.QueryJobConfiguration

class BigQueryExecute extends PipelineStagePlugin with JupyterCompleter {

  val version = Utils.getFrameworkVersion

  val snippet = """{
    |  "type": "BigQueryExecute",
    |  "name": "BigQueryExecute",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "inputURI": "hdfs://*.sql"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/execute/#bigqueryexecute")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputURI" :: "sql" :: "sqlParams" :: "authentication" :: "location" :: "params" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")
    val isInputURI = c.hasPath("inputURI")
    val source = if (isInputURI) "inputURI" else "sql"
    val parsedURI = if (isInputURI) getValue[String]("inputURI") |> parseURI("inputURI") _ else Right(new URI(""))
    val inputSQL = if (isInputURI) parsedURI |> textContentForURI("inputURI", authentication) _ else Right("")
    val inlineSQL = if (!isInputURI) getValue[String]("sql") |> verifyInlineSQLPolicy("sql") _ else Right("")
    val sqlParams = readMap("sqlParams", c)
    val sql = if (isInputURI) inputSQL else inlineSQL
    val validSQL = sql |> injectSQLParams(source, sqlParams, false) _
    val params = readMap("params", c)
    val location = getOptionalValue[String]("location")
    val jobName = getOptionalValue[String]("jobName")
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, parsedURI, inputSQL, inlineSQL, sql, validSQL, location, jobName, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(parsedURI), Right(inputSQL), Right(inlineSQL), Right(sql), Right(validSQL), Right(location), Right(jobName), Right(invalidKeys)) =>
        val uri = if (isInputURI) Option(parsedURI) else None

        val stage = BigQueryExecuteStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputURI=uri,
          sql=sql,
          sqlParams=sqlParams,
          params=params,
          location=location,
          jobName=jobName,
        )

        uri.foreach { uri => stage.stageDetail.put("inputURI", uri.toString) }
        stage.stageDetail.put("sql", inputSQL)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)
        stage.stageDetail.put("params", params.asJava)
        location.foreach { uri => stage.stageDetail.put("location", location.toString) }
        jobName.foreach { uri => stage.stageDetail.put("jobName", jobName.toString) }

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, parsedURI, inputSQL, inlineSQL, sql, validSQL, location, jobName, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class BigQueryExecuteStage(
    plugin: BigQueryExecute,
    id: Option[String],
    name: String,
    description: Option[String],
    inputURI: Option[URI],
    sql: String,
    sqlParams: Map[String, String],
    location: Option[String],
    jobName: Option[String],
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    BigQueryExecuteStage.execute(this)
  }

}

object BigQueryExecuteStage {

  def execute(stage: BigQueryExecuteStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    // replace sql parameters
    val sql = SQLUtils.injectParameters(stage.sql, stage.sqlParams, false)
    stage.stageDetail.put("sql", sql)

    try {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        val bigquery = BigQueryOptions.getDefaultInstance.getService

        val jobName = "jobId_" + UUID.randomUUID().toString()
        val jobId = JobId.newBuilder()
        stage.location.foreach { location => jobId.setLocation(location) }
        stage.jobName.foreach { jobName => jobId.setJob(jobName) }

        val queryConfig = QueryJobConfiguration.newBuilder(sql)
        queryConfig.setUseLegacySql(false)

        val completedJob = bigquery.create(JobInfo.of(jobId.build(), queryConfig.build())).waitFor()
        Option(completedJob.getStatus.getError) match {
          case Some(error) => throw new Exception(completedJob.getStatus.getError.getMessage)
          case None =>
        }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    None
  }

}
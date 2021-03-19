package ai.tripl.arc.load

import scala.collection.JavaConverters._

import org.apache.spark.sql._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.CloudUtils
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.ExtractUtils
import ai.tripl.arc.util.MetadataUtils
import ai.tripl.arc.util.Utils

class BigQueryLoad extends PipelineStagePlugin with JupyterCompleter {

  val version = ai.tripl.arc.bigquery.BuildInfo.version

  def snippet()(implicit arcContext: ARCContext): String = {
    s"""{
    |  "type": "BigQueryLoad",
    |  "name": "BigQueryLoad",
    |  "environments": [${arcContext.completionEnvironments.map { env => s""""${env}""""}.mkString(", ")}],
    |  "inputView": "inputView",
    |  "table": "dataset.table",
    |  "temporaryGcsBucket": "bucket"
    |}""".stripMargin
  }

  val documentationURI = new java.net.URI(s"${baseURI}/load/#bigqueryload")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputView" :: "saveMode" :: "table" :: "dataset" :: "project" :: "parentProject" :: "temporaryGcsBucket" :: "createDisposition" :: "partitionField" :: "partitionExpirationMs" :: "clusteredFields" :: "allowFieldAddition" :: "allowFieldRelaxation" :: "params" :: "location" :: Nil

    val invalidKeys = checkValidKeys(c)(expectedKeys)
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val authentication = readAuthentication("authentication")
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val params = readMap("params", c)

    val location = getValue[String]("location")
    val table = getValue[String]("table")
    val dataset = getOptionalValue[String]("dataset")
    val project = getOptionalValue[String]("project")
    val parentProject = getOptionalValue[String]("parentProject")
    val temporaryGcsBucket = getValue[String]("temporaryGcsBucket")
    val createDisposition = getValue[String]("createDisposition", default = Some("CREATE_IF_NEEDED"))
    val partitionField = getOptionalValue[String]("partitionField")
    val partitionExpirationMs = getOptionalValue[String]("partitionExpirationMs")
    val clusteredFields = getOptionalValue[String]("clusteredFields")
    val allowFieldAddition = getValue[java.lang.Boolean]("allowFieldAddition", default = Some(false))
    val allowFieldRelaxation = getValue[java.lang.Boolean]("allowFieldRelaxation", default = Some(false))

    (id, name, description, saveMode, inputView, location, table, dataset, project, parentProject, temporaryGcsBucket, createDisposition, partitionField, partitionExpirationMs,
      clusteredFields, allowFieldAddition, allowFieldRelaxation, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(saveMode), Right(inputView), Right(location), Right(table), Right(dataset), Right(project), Right(parentProject), Right(temporaryGcsBucket),
              Right(createDisposition), Right(partitionField), Right(partitionExpirationMs), Right(clusteredFields), Right(allowFieldAddition), Right(allowFieldRelaxation), Right(invalidKeys)) =>

       val stage = BigQueryLoadStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          inputView=inputView,
          saveMode=saveMode,
          location=location,
          table=table,
          dataset=dataset,
          project=project,
          parentProject=parentProject,
          temporaryGcsBucket=temporaryGcsBucket,
          createDisposition=createDisposition,
          partitionField=partitionField,
          partitionExpirationMs=partitionExpirationMs,
          clusteredFields=clusteredFields,
          allowFieldAddition=allowFieldAddition,
          allowFieldRelaxation=allowFieldRelaxation,
          params=params
        )

        dataset.foreach { project => stage.stageDetail.put("dataset", dataset) }
        project.foreach { project => stage.stageDetail.put("project", project) }
        stage.stageDetail.put("allowFieldAddition", allowFieldAddition)
        stage.stageDetail.put("allowFieldRelaxation", allowFieldRelaxation)
        stage.stageDetail.put("createDisposition", createDisposition)
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("intermediateFormat", "parquet")
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)
        stage.stageDetail.put("table", table)
        stage.stageDetail.put("location", location)
        stage.stageDetail.put("temporaryGcsBucket", temporaryGcsBucket)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, inputView, saveMode, invalidKeys, location, table, dataset, project, parentProject, temporaryGcsBucket, createDisposition, partitionField, partitionExpirationMs,
                                      clusteredFields, allowFieldAddition, allowFieldRelaxation).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class BigQueryLoadStage(
  plugin: BigQueryLoad,
  id: Option[String],
  name: String,
  description: Option[String],
  inputView: String,
  saveMode: SaveMode,
  location: String,
  table: String,
  dataset: Option[String],
  project: Option[String],
  parentProject: Option[String],
  temporaryGcsBucket: String,
  createDisposition: String,
  partitionField: Option[String],
  partitionExpirationMs: Option[String],
  clusteredFields: Option[String],
  allowFieldAddition: Boolean,
  allowFieldRelaxation: Boolean,
  params: Map[String, String]
) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    BigQueryLoadStage.execute(this)
  }
}

object BigQueryLoadStage {

  def execute(stage: BigQueryLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import stage._

    val df = spark.table(stage.inputView)

    try {
      if (df.isStreaming) {
          throw new Exception("BigQueryLoad does not support streaming mode.")
      } else {
        val options = collection.mutable.HashMap[String, String]()

        options += "temporaryGcsBucket" -> temporaryGcsBucket
        options += "createDisposition" -> createDisposition
        options += "allowFieldAddition" -> allowFieldAddition.toString
        options += "allowFieldRelaxation" -> allowFieldRelaxation.toString
        dataset.foreach( options += "dataset" -> _ )
        project.foreach( options += "project" -> _ )
        parentProject.foreach( options += "parentProject" -> _ )
        partitionField.foreach( options += "partitionField" -> _ )
        partitionExpirationMs.foreach( options += "partitionExpirationMs" -> _ )

        df.write.mode(stage.saveMode).format("bigquery").options(options).save(table)
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    Option(df)
  }

}
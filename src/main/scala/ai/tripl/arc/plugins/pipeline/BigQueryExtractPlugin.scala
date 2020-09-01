package ai.tripl.arc.plugins.pipeline

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

class BigQueryExtract extends PipelineStagePlugin with JupyterCompleter {

  val version = ai.tripl.arc.bigquery.BuildInfo.version

  val snippet = """{
    |  "type": "BigQueryExtract",
    |  "name": "BigQueryExtract",
    |  "environments": [
    |    "production",
    |    "test"
    |  ],
    |  "table": "dataset.table",
    |  "outputView": "outputView"
    |}""".stripMargin

  val documentationURI = new java.net.URI(s"${baseURI}/extract/#bigqueryextract")

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "outputView" ::  "numPartitions" :: "partitionBy" :: "persist" :: "schemaURI" :: "schemaView" :: "table" :: "dataset" :: "project" :: "parentProject" :: "maxParallelism" :: "viewsEnabled" :: "viewMaterializationProject" :: "viewMaterializationDataset" :: "optimizedEmptyProjection" :: "params" :: Nil

    val invalidKeys = checkValidKeys(c)(expectedKeys)
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val params = readMap("params", c)
    val description = getOptionalValue[String]("description")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val authentication = readAuthentication("authentication")

    val extractColumns = if(c.hasPath("schemaURI")) getValue[String]("schemaURI") |> parseURI("schemaURI") _ |> textContentForURI("schemaURI", authentication) |> getExtractColumns("schemaURI") _ else Right(List.empty)
    val schemaView = if(c.hasPath("schemaView")) getValue[String]("schemaView") else Right("")

    val table = getValue[String]("table")
    val dataset = getOptionalValue[String]("dataset")
    val project = getOptionalValue[String]("project")
    val parentProject = getOptionalValue[String]("parentProject")
    val maxParallelism = getOptionalValue[Int]("maxParallelism")
    val viewsEnabled = getOptionalValue[java.lang.Boolean]("viewsEnabled")
    val viewMaterializationProject = getOptionalValue[String]("viewMaterializationProject")
    val viewMaterializationDataset = getOptionalValue[String]("viewMaterializationDataset")
    val optimizedEmptyProjection = getOptionalValue[java.lang.Boolean]("optimizedEmptyProjection")

    (id, name, description, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, table, dataset, project, parentProject, maxParallelism, viewsEnabled, viewMaterializationProject, viewMaterializationDataset, optimizedEmptyProjection, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(extractColumns), Right(schemaView), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(table), Right(dataset), Right(project), Right(parentProject), Right(maxParallelism), Right(viewsEnabled), Right(viewMaterializationProject), Right(viewMaterializationDataset), Right(optimizedEmptyProjection), Right(invalidKeys)) =>

        val schema = if(c.hasPath("schemaView")) Left(schemaView) else Right(extractColumns)

        val stage = BigQueryExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          schema=schema,
          outputView=outputView,
          table=table,
          dataset=dataset,
          project=project,
          parentProject=parentProject,
          maxParallelism=maxParallelism,
          viewsEnabled=viewsEnabled,
          viewMaterializationProject=viewMaterializationProject,
          viewMaterializationDataset=viewMaterializationDataset,
          optimizedEmptyProjection=optimizedEmptyProjection,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        stage.stageDetail.put("table", table)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, extractColumns, schemaView, outputView, persist, numPartitions, partitionBy, table, dataset, project, parentProject, maxParallelism, viewsEnabled, viewMaterializationProject, viewMaterializationDataset, optimizedEmptyProjection, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class BigQueryExtractStage(
  plugin: BigQueryExtract,
  id: Option[String],
  name: String,
  description: Option[String],
  schema: Either[String, List[ExtractColumn]],
  outputView: String,
  table: String,
  dataset: Option[String],
  project: Option[String],
  parentProject: Option[String],
  maxParallelism: Option[Int],
  viewsEnabled: Option[java.lang.Boolean],
  viewMaterializationProject: Option[String],
  viewMaterializationDataset: Option[String],
  optimizedEmptyProjection: Option[java.lang.Boolean],
  params: Map[String, String],
  persist: Boolean,
  numPartitions: Option[Int],
  partitionBy: List[String]
) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    BigQueryExtractStage.execute(this)
  }
}

object BigQueryExtractStage {

  def execute(stage: BigQueryExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import stage._

    // try to get the schema
    val optionSchema = try {
      ExtractUtils.getSchema(schema)(spark, logger)
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stageDetail
      }
    }

    // if incoming dataset is empty create empty dataset with a known schema
    val df = try {
      if (arcContext.isStreaming) {
          throw new Exception("BigQueryExtract does not support streaming mode.")
      } else {
        val options = collection.mutable.HashMap[String, String]()

        dataset.foreach( options += "dataset" -> _ )
        project.foreach( options += "project" -> _ )
        parentProject.foreach( options += "parentProject" -> _ )
        maxParallelism.foreach( options += "maxParallelism" -> _.toString )
        viewsEnabled.foreach( options += "viewsEnabled" -> _.toString )
        viewMaterializationProject.foreach( options += "viewMaterializationProject" -> _ )
        viewMaterializationDataset.foreach( options += "viewMaterializationDataset" -> _ )
        optimizedEmptyProjection.foreach( options += "optimizedEmptyProjection" -> _.toString )

        spark.read.format("bigquery").options(options).load(table)
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // set column metadata if exists
    val enrichedDF = optionSchema match {
        case Some(schema) => MetadataUtils.setMetadata(df, schema)
        case None => df
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions)
          case None => enrichedDF
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => enrichedDF(col))
        stage.numPartitions match {
          case Some(numPartitions) => enrichedDF.repartition(numPartitions, partitionCols:_*)
          case None => enrichedDF.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("outputColumns", Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}


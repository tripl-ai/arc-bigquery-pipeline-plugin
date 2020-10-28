package ai.tripl.arc.load

import scala.util._

import org.apache.commons.lang3.StringUtils

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.google.api.gax.rpc.AlreadyExistsException
import com.google.cloud.datacatalog.v1._

import ai.tripl.arc.util.ControlUtils._


object DataCatalog {

    case class DataCatalogContext(location: String, projectId: String, entryGroupId: String, entryId: String)

    def createEntryGroup(displayName: String, description: String)(implicit dcCxt: DataCatalogContext) {
        import dcCxt._

        using(DataCatalogClient.create()) { client =>
            val res = Try {
                val entryGroup = EntryGroup.newBuilder()
                                    .setDisplayName(displayName)
                                    .setDescription(description)
                                    .build()

                val entryGroupRequest = CreateEntryGroupRequest.newBuilder()
                                            .setParent(LocationName.of(projectId, location).toString())
                                            .setEntryGroupId(entryGroupId)
                                            .setEntryGroup(entryGroup)
                                            .build()

                client.createEntryGroup(entryGroupRequest)
            }

            res match {
                case Success(entryGroupResponse) =>
                    println("\nEntry Group created with name: %s\n", entryGroupResponse.getName())
                case Failure(e: AlreadyExistsException) =>
                    println("\nEntry Group already exists\n")
                case Failure(e) =>
                    throw new Exception(e)
            }
        }
    }

    def createEntry(displayName: String, description: String, datasetId: String, tableId: String, sparkSchema: StructType, update: Boolean = false)(implicit dcCxt: DataCatalogContext) {
        import dcCxt._

        using(DataCatalogClient.create()) { client =>
            val res = Try {
                val schema = schemaFromSparkSchema(sparkSchema)
                val entry = entryWithSchema(displayName, description, projectId, datasetId, tableId, schema)

                if (update) {
                    val entryName = EntryName.of(projectId, location, entryGroupId, entryId).toString()
                    val deleteRequest = DeleteEntryRequest.newBuilder().setName(entryName).build()
                    client.deleteEntry(deleteRequest)

                }

                val entryRequest = CreateEntryRequest.newBuilder()
                                    .setParent(EntryGroupName.of(projectId, location, entryGroupId).toString())
                                    .setEntryId(entryId)
                                    .setEntry(entry)
                                    .build()

                client.createEntry(entryRequest)
            }

            res match {
                case Success(entryResponse) =>
                    println("\nEntry created with name: %s\n", entryResponse.getName())
                case Failure(e: AlreadyExistsException) =>
                    println("\nEntry already exists\n")
                    if (!update) {
                        createEntry(displayName, description, datasetId, tableId, sparkSchema, true)
                    }
                case Failure(e) =>
                    throw new Exception(e)
            }
        }
    }


    def schemaFromSparkSchema(s: StructType): Schema = {
       val b = Schema.newBuilder()

       for (f <- s) {
           val cs = ColumnSchema.newBuilder()
           cs.setColumn(f.name)
           if (f.nullable) {
               cs.setMode("NULLABLE")
           } else {
               cs.setMode("REQUIRED")
           }
           cs.setType(f.dataType.catalogString)

           val metadata = f.metadata

           if (metadata.contains("description")) {
             val builder = new StringBuilder()
             if (metadata.contains("classification")) {
                 val classification = metadata.getMetadata("classification")
                 if (classification.contains("is_pii")) {
                     val pii = classification.getBoolean("is_pii")
                     if (pii) {
                         builder.append("PII | ")
                     }
                 }
                 if (classification.contains("level")) {
                     val level = classification.getString("level")
                     builder.append(level)
                     builder.append(" | ")
                 }
             }
             val desc = f.metadata.getString("description")
             builder.append(desc)

             cs.setDescription(builder.toString)
           }

           b.addColumns(cs.build)
       }

       b.build
    }

    def entryWithSchema(displayName: String, description: String, projectId: String, datasetId: String, tableId: String, schema: Schema): Entry = {
        val b = Entry.newBuilder()
        b.setDisplayName(displayName)
        b.setDescription(description)
        b.setSchema(schema)
        b.setLinkedResource(s"//bigquery.googleapis.com/projects/${projectId}/datasets/${datasetId}/tables/${tableId}")
        b.build
    }

}
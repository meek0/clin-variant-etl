package bio.ferlab.clin.etl.utils

import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, transform => colTransform}


/**
 * Transformation to rename fields within each struct of an array column in a DataFrame.
 *
 * @param arrayCol  The name of the array column containing structs whose fields should be renamed.
 * @param renameMap A mapping from existing field names to their new names.
 *
 * For each struct in the specified array column, this transformation will replace
 * the existing field names with the new names provided in the `rename_map`.
 *
 * Example: to rename nested field donors.sequencing_id to donors.service_request_id, where donors is an array of structs column:
 *   val transformation = RenameFieldsInArrayStruct("donors", Map("sequencing_id" -> "service_request_id"))
 *   val transformedDf = transformation.transform(df)
 * }}}
*/
case class RenameFieldsInArrayStruct(arrayCol: String, renameMap: Map[String, String]) extends Transformation {

  override def transform: DataFrame => DataFrame = { df =>
    renameMap.foldLeft(df) {
      case (d, (existingName, newName)) =>
        d.withColumn(arrayCol, colTransform(
          col(arrayCol),
          (c: Column) => c.withField(newName, c(existingName)).dropFields(existingName)
        ))
    }
  }
}

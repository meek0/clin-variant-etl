package bio.ferlab.clin.etl.varsome

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{ArrayType, StructType}

object VarsomeUtils {

  val transformPartition: (String, String) => Iterator[Row] => Iterator[VarsomeResponse] = (url, token) => rows => {
    val locuses = rows.map(r => r.getAs[String]("locus")).toSeq
    if (locuses.isEmpty) Iterator.empty[VarsomeResponse]
    else {
      val response = VarsomeHttpClient(url, token).getEntities(locuses)
      Iterator(response)
    }
  }

  val varsomeSchema = new ArrayType(ScalaReflection.schemaFor[VarsomeEntity].dataType.asInstanceOf[StructType], false)

}

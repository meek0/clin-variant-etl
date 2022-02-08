package bio.ferlab.clin.etl.varsome

import bio.ferlab.datalake.commons.config.DatasetConf
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object VarsomeUtils {

  val transformPartition: (String, String) => Iterator[Row] => Iterator[VarsomeResponse] = (url, token) => rows => {
    val locuses: String = rows.map(r => r.getAs[String]("locus")).toSeq.mkString(",")
    val response = VarsomeHttpClient(url, token).getEntities(locuses)
    Iterator(response)
  }


  def tableExist(datasetConf: DatasetConf)(implicit spark: SparkSession): Boolean = {
    datasetConf.table.exists(t => spark.catalog.listTables(t.database).where(col("name") === t.name).count() > 0)
  }


  val varsomeSchema = new ArrayType(ScalaReflection.schemaFor[VarsomeEntity].dataType.asInstanceOf[StructType], false)
  //  val varsomeSchema = new ArrayType(new StructType()
  //    .add("chromosome", StringType)
  //    .add("ref", StringType)
  //    .add("alt", StringType)
  //    .add("variant_id", StringType)
  //    .add("pos", LongType), false)

}

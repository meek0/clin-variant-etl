package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirToNormalizedETL.getSchema
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.RawToNormalizedETL
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime
import scala.io.Source

class FhirToNormalizedETL(override val source: DatasetConf,
                          override val destination: DatasetConf,
                          override val transformations: List[Transformation])
                         (override implicit val conf: Configuration)  extends RawToNormalizedETL(source, destination, transformations){

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    log.info(s"extracting: ${source.location}")
    val schema = getSchema(source.id)
    Map(source.id -> spark.read.schema(schema).format(source.format.sparkFormat).options(source.readoptions).load(source.location))
  }

}

object FhirToNormalizedETL{
  def getSchema(schema: String): StructType = {
    val url = ClassLoader.getSystemResource(s"fhir_schemas/$schema.json")
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    schemaFromJson
  }
}

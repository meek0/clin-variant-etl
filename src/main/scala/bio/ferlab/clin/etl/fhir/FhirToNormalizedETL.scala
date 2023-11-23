package bio.ferlab.clin.etl.fhir

import bio.ferlab.clin.etl.fhir.FhirToNormalizedETL.getSchema
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.TransformationsETL
import bio.ferlab.datalake.spark3.transformation.Transformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}

import java.time.LocalDateTime
import scala.io.Source

case class FhirToNormalizedETL(rc: DeprecatedRuntimeETLContext,
                               override val source: DatasetConf,
                               override val mainDestination: DatasetConf,
                               override val transformations: List[Transformation])
  extends TransformationsETL(rc, source, mainDestination, transformations) {

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    log.info(s"extracting: ${source.location}")
    val schema = getSchema(source.id)
    Map(source.id -> spark.read.schema(schema).format(source.format.sparkFormat).options(source.readoptions).load(source.location))
  }

}

object FhirToNormalizedETL {
  def getSchema(schema: String): StructType = {
    val schemaSource = Source.fromResource(s"fhir_schemas/$schema.json").getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    schemaFromJson
  }
}

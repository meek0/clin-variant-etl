package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.mainutils.OptionalBatch
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext, RepartitionByColumns}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.GenomicOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locusColumnNames
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class SNVSomatic(rc: DeprecatedRuntimeETLContext, batchId: Option[String]) extends SingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_snv_somatic")
  val normalized_snv_somatic: DatasetConf = conf.getDataset("normalized_snv_somatic")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  override def extract(lastRunDateTime: LocalDateTime = minDateTime,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    import spark.implicits._

    batchId match {
      // If a batch id was submitted, only process the specified id
      case Some(id) =>
        val normalizedSnvSomaticDf = normalized_snv_somatic.read.where($"batch_id" === id)
        val analysisServiceRequestIds: Seq[String] = enriched_clinical.read.where($"batch_id" === id)
          .select("analysis_service_request_id")
          .distinct()
          .as[String]
          .collect()

        val fs = FileSystemResolver.resolve(conf.getStorage(mainDestination.storageid).filesystem)
        val destinationExists = fs.exists(mainDestination.location) && mainDestination.tableExist

        val enrichedSnvSomaticDf = if (destinationExists) {
          mainDestination.read.where($"analysis_service_request_id".isin(analysisServiceRequestIds: _*))
        } else spark.emptyDataFrame

        Map(
          normalized_snv_somatic.id -> normalizedSnvSomaticDf,
          mainDestination.id -> enrichedSnvSomaticDf
        )
      case None =>
        // If no batch id were submitted, process all data
        Map(
          normalized_snv_somatic.id -> normalized_snv_somatic.read,
          mainDestination.id -> spark.emptyDataFrame // No need to union with enriched
        )
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minDateTime,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val normalizedSnvSomaticDf = data(normalized_snv_somatic.id)
    val enrichedSnvSomaticDf = data(mainDestination.id)

    val withPastAnalysesDf = if (!enrichedSnvSomaticDf.isEmpty)
      normalizedSnvSomaticDf
        .unionByName(enrichedSnvSomaticDf.drop("all_analyses"), allowMissingColumns = true)
        .distinct()
    else normalizedSnvSomaticDf // No past analyses since enriched is empty for this service request id

    val withAllAnalysesDf = withPastAnalysesDf
      .groupByLocus($"aliquot_id")
      .agg(collect_set($"bioinfo_analysis_code") as "all_bioinfo_analysis_codes")
      .withColumn("all_analyses", when(array_contains($"all_bioinfo_analysis_codes", "TEBA"), array(lit("TO"))).otherwise(array()))
      .withColumn("all_analyses", when(array_contains($"all_bioinfo_analysis_codes", "TNEBA"), array_union($"all_analyses", array(lit("TN")))).otherwise($"all_analyses"))
      .drop("all_bioinfo_analysis_codes")

    withPastAnalysesDf
      .join(withAllAnalysesDf, locusColumnNames :+ "aliquot_id", "inner")
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("analysis_service_request_id", "chromosome"), n = Some(1))
}

object SNVSomatic {
  @main
  def run(rc: DeprecatedRuntimeETLContext, batch: OptionalBatch): Unit = {
    SNVSomatic(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

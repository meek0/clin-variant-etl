package bio.ferlab.clin.etl.enriched

import bio.ferlab.clin.etl.mainutils.OptionalBatch
import bio.ferlab.clin.etl.utils.ClinicalUtils.getAnalysisServiceRequestIdsInBatch
import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.GenomicOperations
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns.locusColumnNames
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.LocalDateTime

case class SNVSomatic(rc: RuntimeETLContext, batchId: Option[String]) extends SimpleSingleETL(rc) {

  override val mainDestination: DatasetConf = conf.getDataset("enriched_snv_somatic")
  val normalized_snv_somatic: DatasetConf = conf.getDataset("normalized_snv_somatic")
  val normalized_cnv: DatasetConf = conf.getDataset("normalized_cnv")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  override def extract(lastRunDateTime: LocalDateTime = minValue,
                       currentRunDateTime: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    import spark.implicits._

    batchId match {
      // If a batch id was submitted, only process the specified id
      case Some(id) =>
        val normalizedSnvSomaticDf = normalized_snv_somatic.read.where($"batch_id" === id)
        val normalized_cnvDf = normalized_cnv.read.where($"batch_id" === id)
        val clinicalDf = enriched_clinical.read
        val analysisServiceRequestIds: Seq[String] = getAnalysisServiceRequestIdsInBatch(clinicalDf, id)

        val fs = FileSystemResolver.resolve(conf.getStorage(mainDestination.storageid).filesystem)
        val destinationExists = fs.exists(mainDestination.location) && mainDestination.tableExist

        val enrichedSnvSomaticDf = if (destinationExists) {
          mainDestination.read.where($"analysis_service_request_id".isin(analysisServiceRequestIds: _*))
        } else spark.emptyDataFrame

        Map(
          normalized_snv_somatic.id -> normalizedSnvSomaticDf,
          normalized_cnv.id -> normalized_cnvDf,
          mainDestination.id -> enrichedSnvSomaticDf
        )
      case None =>
        // If no batch id were submitted, process all data
        Map(
          normalized_snv_somatic.id -> normalized_snv_somatic.read,
          normalized_cnv.id -> normalized_cnv.read,
          mainDestination.id -> spark.emptyDataFrame // No need to union with enriched
        )
    }
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime = minValue,
                               currentRunDateTime: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val normalizedSnvSomaticDf = data(normalized_snv_somatic.id)
    val normalizedCnvDf = data(normalized_cnv.id)
    val enrichedSnvSomaticDf = data(mainDestination.id)

    val withPastAnalysesDf = if (!enrichedSnvSomaticDf.isEmpty)
      normalizedSnvSomaticDf
        .unionByName(enrichedSnvSomaticDf.drop("all_analyses").drop("cnv_count"), allowMissingColumns = true)
        .distinct()
    else normalizedSnvSomaticDf // No past analyses since enriched is empty for this service request id

    val withSnvCount = withCount(withPastAnalysesDf, "hgvsg", normalizedCnvDf, "name", "cnv_count")

    val withAllAnalysesDf = withPastAnalysesDf
      .groupByLocus($"aliquot_id")
      .agg(collect_set($"bioinfo_analysis_code") as "all_bioinfo_analysis_codes")
      .withColumn("all_analyses", when(array_contains($"all_bioinfo_analysis_codes", "TEBA"), array(lit("TO"))).otherwise(array()))
      .withColumn("all_analyses", when(array_contains($"all_bioinfo_analysis_codes", "TNEBA"), array_union($"all_analyses", array(lit("TN")))).otherwise($"all_analyses"))
      .drop("all_bioinfo_analysis_codes")

    withSnvCount
      .join(withAllAnalysesDf, locusColumnNames :+ "aliquot_id", "inner")
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("analysis_service_request_id", "chromosome"), n = Some(100), sortColumns = Seq("start"))
}

object SNVSomatic {
  @main
  def run(rc: RuntimeETLContext, batch: OptionalBatch): Unit = {
    SNVSomatic(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

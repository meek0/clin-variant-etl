package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.fhir.GenomicFile.EXOMISER_CNV
import bio.ferlab.clin.etl.mainutils.{AnalysisIds, Batch}
import bio.ferlab.clin.etl.model.raw.{RawExomiser, RawExomiserCNV, RawFranklin}
import bio.ferlab.clin.etl.utils.ClinicalUtils.getAnalysisIdsInBatch
import bio.ferlab.clin.etl.utils.{FileInfo, FileUtils}
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.transformation.Cast.{castFloat, castInt, castLong}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{element_at, input_file_name, lit, split}
import org.apache.spark.sql.types.BooleanType
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._

import java.time.LocalDateTime

case class ExomiserCNV(rc: RuntimeETLContext, analysisIds: Seq[String] = Seq()) extends SimpleSingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("normalized_exomiser_cnv")
  val raw_exomiser_cnv: DatasetConf = conf.getDataset("raw_exomiser_cnv")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {

    val exomiserCNVFiles: Set[FileInfo] = FileUtils.fileUrls(analysisIds, EXOMISER_CNV)

    Map(
      raw_exomiser_cnv.id -> {
        if (exomiserCNVFiles.isEmpty) Seq.empty[RawExomiser].toDF() else
          spark.read
            .format(raw_exomiser_cnv.format.sparkFormat)
            .options(raw_exomiser_cnv.readoptions)
            .load(paths = exomiserCNVFiles.map(_.url).toSeq: _*)
      },
      "file_info" -> exomiserCNVFiles.toSeq.toDF()
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime): DataFrame = {
    val fileInfo = data("file_info").select("url", "aliquot_id", "batch_id", "analysis_id")

    val withAliquotId = data(raw_exomiser_cnv.id)
      .withColumn("url", input_file_name())
      .join(fileInfo, Seq("url"))
      .drop("url")

    withAliquotId
      .select($"CONTIG" as "chromosome",
        castLong("START") as "start",
        castLong("END") as "end",
        $"REF" as "reference",
        $"ALT" as "alternate",
        $"ID" as "id",
        $"aliquot_id",
        castInt("#RANK") as "rank",
        $"GENE_SYMBOL" as "gene_symbol",
        castLong("ENTREZ_GENE_ID") as "entrez_gene_id",
        castFloat("EXOMISER_VARIANT_SCORE") as "exomiser_variant_score",
        castFloat("EXOMISER_GENE_COMBINED_SCORE") as "gene_combined_score",
        $"CONTRIBUTING_VARIANT".cast(BooleanType) as "contributing_variant",
        $"MOI" as "moi",
        $"EXOMISER_ACMG_CLASSIFICATION" as "acmg_classification",
        split($"EXOMISER_ACMG_EVIDENCE", ",") as "acmg_evidence",
        $"batch_id",
        $"analysis_id"
      )
  }

}

object ExomiserCNV {
  @main
  def run(rc: RuntimeETLContext, analysisIds: AnalysisIds): Unit = {
    ExomiserCNV(rc, analysisIds.ids).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

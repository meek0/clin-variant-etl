package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.fhir.GenomicFile.EXOMISER
import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.model.raw.RawExomiser
import bio.ferlab.clin.etl.utils.{FileInfo, FileUtils}
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.transformation.Cast.{castFloat, castInt, castLong}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{input_file_name, lit, split}
import org.apache.spark.sql.types.BooleanType

import java.time.LocalDateTime

case class Exomiser(rc: DeprecatedRuntimeETLContext, batchId: String) extends SingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("normalized_exomiser")
  val raw_exomiser: DatasetConf = conf.getDataset("raw_exomiser")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    val exomiserFiles: Set[FileInfo] = FileUtils.fileUrls(batchId, EXOMISER)

    Map(
      raw_exomiser.id -> {
        if (exomiserFiles.isEmpty) Seq.empty[RawExomiser].toDF() else
          spark.read
            .format(raw_exomiser.format.sparkFormat)
            .options(raw_exomiser.readoptions)
            .load(paths = exomiserFiles.map(_.url).toSeq: _*)
      },
      "file_info" -> exomiserFiles.toSeq.toDF()
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime): DataFrame = {
    val fileInfo = data("file_info").select("url", "aliquot_id")
    val withAliquotId = data(raw_exomiser.id)
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
        lit(batchId) as "batch_id"
      )
  }

  override def replaceWhere: Option[String] = Some(s"batch_id = '$batchId'")
}

object Exomiser {
  @main
  def run(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = {
    Exomiser(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.utils.FileUtils
import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf}
import bio.ferlab.datalake.spark3.etl.ETLSingleDestination
import bio.ferlab.datalake.spark3.transformation.Cast.{castFloat, castInt, castLong}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDateTime

class Exomiser(batchId: String)(implicit configuration: Configuration) extends ETLSingleDestination {
  override val mainDestination: DatasetConf = conf.getDataset("normalized_exomiser")
  val raw_exomiser: DatasetConf = conf.getDataset("raw_exomiser")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): Map[String, DataFrame] = {
    import spark.implicits._
    val exomiserFiles = FileUtils.filesUrl(batchId, "EXOMISER", "TSV")

    Map(
      raw_exomiser.id -> spark.read
        .format(raw_exomiser.format.sparkFormat)
        .options(raw_exomiser.readoptions)
        .load(paths = exomiserFiles.map(_.url).toSeq: _*),
      "file_info" -> exomiserFiles.toSeq.toDF()
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
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
        $"EXOMISER_ACMG_EVIDENCE" as "acmg_evidence",
      )
  }
}

package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.model.raw.RawFranklin
import bio.ferlab.clin.etl.normalized.Franklin._
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import java.time.LocalDateTime

case class Franklin(rc: RuntimeETLContext, batchId: String) extends SimpleSingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("normalized_franklin")
  val raw_franklin: DatasetConf = conf.getDataset("raw_franklin")
  val enriched_clinical: DatasetConf = conf.getDataset("enriched_clinical")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    val runtimePath = raw_franklin.path.replace("{{BATCH_ID}}", batchId)
    val runtimeSourceDs = raw_franklin.copy(path = runtimePath)
    val fs = FileSystemResolver.resolve(conf.getStorage(raw_franklin.storageid).filesystem)
    val sourceExists = fs.exists(runtimeSourceDs.location)

    val sourceDf = if (sourceExists) {
      // If there are still .txt files, it means Franklin's analysis is not done. Use empty DF.
      val files = fs.list(runtimeSourceDs.location, recursive = true)
      if (files.exists(_.path.endsWith(".txt"))) {
        log.warn("Franklin analysis is not completed yet. Using empty DataFrame.")
        Seq.empty[RawFranklin].toDF()
      } else runtimeSourceDs.read
    } else Seq.empty[RawFranklin].toDF()

    Map(
      raw_franklin.id -> sourceDf,
      enriched_clinical.id -> enriched_clinical.read
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime): DataFrame = {

    data(raw_franklin.id)
      .select(
        explode($"variants") as "variant",
        ltrim($"variant.variant.chromosome", "chr") as "chromosome",
        $"variant.variant.start_position" as "start",
        $"variant.variant.end_position" as "end",
        $"variant.variant.ref" as "reference",
        $"variant.variant.alt" as "alternate",
        lit(batchId) as "batch_id",
        parseNullString("family_id"),
        parseNullString("aliquot_id"),
        $"analysis_id".cast(StringType) as "franklin_analysis_id", // Spark infers it as int but should be String
        $"variant.priority.score" as "score",
        $"variant.classification.acmg_classification" as "acmg_classification",
        $"variant.variant_franklin_link" as "link",
        functions.transform(
          filter($"variant.classification.acmg_rules", col => col("is_met")), col => col("name")
        ) as "acmg_evidence"
      )
      .drop("variant")
      .withClinicalData(data(enriched_clinical.id))
  }
}

object Franklin {

  def parseNullString(columnName: String): Column =
    when(col(columnName) === "null", lit(null).cast(StringType)).otherwise(col(columnName)) as columnName

  implicit class DataFrameOps(df: DataFrame) {

    def withClinicalData(clinicalDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
      val clinicalDataDf = clinicalDf
        .select(
          col("aliquot_id").as("clinical_aliquot_id"),
          col("family_id").as("clinical_family_id"),
          col("analysis_id"))
        .distinct()

      // Join by either family id or aliquot id
      val joinCondition = Seq(
        df("family_id").isNotNull && df("family_id") === clinicalDataDf("clinical_family_id"),
        df("aliquot_id").isNotNull && df("aliquot_id") === clinicalDataDf("clinical_aliquot_id")
      ).reduce(_ || _)

      df.join(clinicalDataDf, joinCondition, "left")
        .drop("clinical_family_id", "clinical_aliquot_id")
    }
  }

  @main
  def run(rc: RuntimeETLContext, batch: Batch): Unit = {
    Franklin(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

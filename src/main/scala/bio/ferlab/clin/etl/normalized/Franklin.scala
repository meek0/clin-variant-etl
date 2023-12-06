package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.model.raw.RawFranklin
import bio.ferlab.clin.etl.normalized.Franklin.parseNullString
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, functions}

import java.time.LocalDateTime

case class Franklin(rc: RuntimeETLContext, batchId: String) extends SimpleSingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("normalized_franklin")
  val raw_franklin: DatasetConf = conf.getDataset("raw_franklin")

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

    Map(raw_franklin.id -> sourceDf)
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
        $"analysis_id".cast(StringType) as "analysis_id", // Spark infers it as int but should be String
        $"variant.priority.score" as "franklin_score",
        $"variant.classification.acmg_classification" as "franklin_acmg_classification",
        $"variant.variant_franklin_link" as "franklin_link",
        functions.transform(
          filter($"variant.classification.acmg_rules", col => col("is_met")), col => col("name")
        ) as "franklin_acmg_evidence"
      )
      .drop("variant")
  }
}

object Franklin {

  def parseNullString(columnName: String): Column =
    when(col(columnName) === "null", lit(null).cast(StringType)).otherwise(col(columnName)) as columnName

  @main
  def run(rc: RuntimeETLContext, batch: Batch): Unit = {
    Franklin(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

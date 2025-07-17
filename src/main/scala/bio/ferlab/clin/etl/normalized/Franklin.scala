package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.AnalysisIds
import bio.ferlab.clin.etl.model.raw.RawFranklin
import bio.ferlab.clin.etl.normalized.Franklin._
import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.commons.file.FileSystemResolver
import bio.ferlab.datalake.spark3.etl.v4.SimpleSingleETL
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame, functions}

import java.time.LocalDateTime

case class Franklin(rc: RuntimeETLContext, analysisIds: Seq[String] = Seq()) extends SimpleSingleETL(rc) {

  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("normalized_franklin")
  val raw_franklin: DatasetConf = conf.getDataset("raw_franklin")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {

    val fs = FileSystemResolver.resolve(conf.getStorage(raw_franklin.storageid).filesystem)
    val schema = Seq.empty[RawFranklin].toDF().schema

    val analysisDFs: Seq[DataFrame] = analysisIds
      .map(id => (id, raw_franklin.replacePath("{{ANALYSIS_ID}}", id)))
      .filter { case (id, ds) => { // analysis that are not ready are filtered out
        val location = ds.location
        check(
          fs.exists(location),
          s"Expected analysis location does not exist for analysis $id: $location"
        ) &&
          check(
            !fs.list(location, recursive = true).exists(_.path.endsWith(".txt")),
            s"Franklin analysis is not completed yet for analysis $id according to our inspection of the analysis location: $location"
          )
      }
      }
      .map { case (id, ds) => // read the analysis data and add the analysis_id column

        // using a fixed schema as there can be mild differences on auto-inferred schemas which will prevent using the union below
        spark.read.schema(schema).options(ds.readoptions).format(ds.format.sparkFormat).load(ds.location)
          .withColumn("analysis_id", lit(id))
      }

    val sourceDf = if (analysisDFs.nonEmpty) {
      analysisDFs.reduce(_ union _)
    } else {
      log.warn("No valid Franklin analysis found for the provided analysis ids.")
      Seq.empty[RawFranklin].toDF()
    }

    Map(
      raw_franklin.id -> sourceDf
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
        parseNullString("aliquot_id"),
        $"analysis_id",
        $"franklin_analysis_id".cast(StringType) as "franklin_analysis_id", // Because spark infers as int but should be String
        $"variant.priority.score" as "score",
        $"variant.classification.acmg_classification" as "acmg_classification",
        $"variant.variant_franklin_link" as "link",
        functions.transform(
          filter($"variant.classification.acmg_rules", col => col("is_met")), col => col("name")
        ) as "acmg_evidence"
      )
      .drop("variant")
  }

  private def check(condition: Boolean, message: String): Boolean = {
    if (!condition) {
      log.warn(message)
    }
    condition
  }
}

object Franklin {

  def parseNullString(columnName: String): Column =
    when(col(columnName) === "null", lit(null).cast(StringType)).otherwise(col(columnName)) as columnName


  @main
  def run(rc: RuntimeETLContext, analysisIds: AnalysisIds): Unit = {
    if (analysisIds.ids.isEmpty) {
      throw new IllegalArgumentException("At least one analysis id must be provided.")
    }

    Franklin(rc, analysisIds.ids).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}

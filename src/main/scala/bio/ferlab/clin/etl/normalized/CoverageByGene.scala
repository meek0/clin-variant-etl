package bio.ferlab.clin.etl.normalized

import bio.ferlab.clin.etl.mainutils.Batch
import bio.ferlab.clin.etl.model.raw.RawCoverageByGene
import bio.ferlab.clin.etl.utils.FileUtils
import bio.ferlab.datalake.commons.config.{DatasetConf, DeprecatedRuntimeETLContext}
import bio.ferlab.datalake.spark3.etl.v3.SingleETL
import bio.ferlab.datalake.spark3.transformation.Cast.{castDouble, castFloat, castInt}
import mainargs.{ParserForMethods, main}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{input_file_name, lit}

import java.time.LocalDateTime

case class CoverageByGene(rc: DeprecatedRuntimeETLContext, batchId: String) extends SingleETL(rc) {
  import spark.implicits._

  override val mainDestination: DatasetConf = conf.getDataset("normalized_coverage_by_gene")
  val raw_coverage_by_gene: DatasetConf = conf.getDataset("raw_coverage_by_gene")

  override def extract(lastRunDateTime: LocalDateTime,
                       currentRunDateTime: LocalDateTime): Map[String, DataFrame] = {
    val coverageByGeneFiles = FileUtils.filesUrl(batchId, "COVGENE", "CSV")

    Map(
      raw_coverage_by_gene.id -> {
        if (coverageByGeneFiles.isEmpty) Seq.empty[RawCoverageByGene].toDF() else
          spark.read
            .format(raw_coverage_by_gene.format.sparkFormat)
            .options(raw_coverage_by_gene.readoptions)
            .load(paths = coverageByGeneFiles.map(_.url).toSeq: _*)
      },
      "file_info" -> coverageByGeneFiles.toSeq.toDF()
    )
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunDateTime: LocalDateTime,
                               currentRunDateTime: LocalDateTime): DataFrame = {
    val fileInfo = data("file_info").select("url", "aliquot_id", "service_request_id", "patient_id")
    val withFileInfo = data(raw_coverage_by_gene.id)
      .withColumn("url", input_file_name())
      .join(fileInfo, Seq("url"))
      .drop("url")

    withFileInfo
      .select(
        $"#gene" as "gene",
        castInt("size"),
        castDouble("average_coverage"),
        castFloat("coverage5"),
        castFloat("coverage15"),
        castFloat("coverage30"),
        castFloat("coverage50"),
        castFloat("coverage100"),
        castFloat("coverage200"),
        castFloat("coverage300"),
        castFloat("coverage400"),
        castFloat("coverage500"),
        castFloat("coverage1000"),
        $"aliquot_id",
        $"patient_id",
        $"service_request_id",
        lit(batchId) as "batch_id"
      )
  }

  override def replaceWhere: Option[String] = Some(s"batch_id = '$batchId'")
}

object CoverageByGene {
  @main
  def run(rc: DeprecatedRuntimeETLContext, batch: Batch): Unit = {
    CoverageByGene(rc, batch.id).run()
  }

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args)
}
